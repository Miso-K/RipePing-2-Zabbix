import sys
import logging
import socket
import struct
import re
try: 
    import simplejson as json
except ImportError: 
    import json
import time
from past.builtins import xrange

__version__ = '1.0.4'

logger = logging.getLogger(__name__)
DEFAULT_SOCKET_TIMEOUT = 60.0
RESPONSE_REGEX_STRING = r'[Pp]rocessed:? (?P<processed>\d+);? [Ff]ailed:? (?P<failed>\d+);? [Tt]otal:? (?P<total>\d+);? [Ss]econds spent:? (?P<seconds>\d+\.\d+)'
RESPONSE_REGEX = re.compile(RESPONSE_REGEX_STRING)
MAX_ITEMS_PER_SEND = 250
PY2 = sys.version_info[0] == 2
STRING_ZABBIX_HEADER = 'ZBXD\1'
BYTE_ZABBIX_HEADER = b'ZBXD\1'
if PY2:
    ZABBIX_HEADER = STRING_ZABBIX_HEADER
else:
    ZABBIX_HEADER = BYTE_ZABBIX_HEADER

class ZabbixInvalidHeaderError(Exception):
    def __init__(self, *args):
        self.raw_response = args[0]
        super(ZabbixInvalidHeaderError, self).__init__(
            u'Invalid header during response from server', *args)
  
    
class ZabbixInvalidResponseError(Exception):
    def __init__(self, *args):
        self.raw_response = args[0]
        super(ZabbixInvalidResponseError, self).__init__(
            u'Invalid response from server')
  
    
class ZabbixPartialSendError(Exception):
    def __init__(self, *args):
        self.response = args[0]
        super(ZabbixPartialSendError, self).__init__(
            u'Some traps failed to be processed')
    
    
class ZabbixTotalSendError(Exception):
    def __init__(self, *args):
        self.response = args[0]
        super(ZabbixTotalSendError, self).__init__(
            u'All traps failed to be processed')


def get_clock(clock=None):
    if clock: return clock
    return int(round(time.time()))


def get_packet(items_as_list_of_dicts):
    return json.dumps({'request': 'sender data',
                       'data': items_as_list_of_dicts,
                       'clock': get_clock()}
                      )
        

def parse_zabbix_response(response):
    match = RESPONSE_REGEX.match(response)
    processed = int(match.group('processed'))
    failed = int(match.group('failed'))
    total = int(match.group('total'))
    seconds = float(match.group('seconds')) 
    return processed,failed,total,seconds
    
    
def parse_raw_response(raw_response):
    return json.loads(raw_response)['info']


def get_data_to_send(packet):
    packet_length = len(packet)
    data_header = struct.pack('q', packet_length)
    if not PY2:
        packet = packet.encode('utf-8')
    return ZABBIX_HEADER + data_header + packet
    
    
def get_raw_response(sock):
    response_data_header = sock.recv(8)
    response_data_header = response_data_header[:4]
    response_len = struct.unpack('i', response_data_header)[0]
    if PY2:
        raw_response = sock.recv(response_len)
    else:
        raw_response = sock.recv(response_len).decode('utf-8')
    return raw_response


def send(packet, 
         server='127.0.0.1', 
         port=10051, 
         timeout=DEFAULT_SOCKET_TIMEOUT):
    socket.setdefaulttimeout(timeout)

    data_to_send = get_data_to_send(packet)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((server, port))
        sock.send(data_to_send)
    except Exception:
        logger.exception(u'Error talking to server')
        raise
    else:
        response_header = sock.recv(5)
        if not response_header == ZABBIX_HEADER:
            raise ZabbixInvalidHeaderError(packet, response_header)        
        raw_response = get_raw_response(sock)
    finally:
        sock.close()
    return ZabbixTrapperResponse(raw_response)
    

class ZabbixTrapperResponse(object):
    def __init__(self, raw_response):
        self.processed = None
        self.failed = None
        self.total = None
        self.seconds = None
        self.items = []
        self.raw_response = raw_response
        response = self.parse_raw_response()
        self.parse_response(response)
        
        
    def parse_response(self, response):
        try:
            (self.processed,
             self.failed,
             self.total,
             self.seconds) = parse_zabbix_response(response)
        except Exception:
            logger.exception('Error parsing decoded response')
            raise ZabbixInvalidResponseError(self.raw_response)
        
        
    def parse_raw_response(self):
        try:
            json_response = json.loads(self.raw_response)
            response = json_response['info']
        except Exception:
            logger.exception('Error parsing raw response')
            raise ZabbixInvalidResponseError(self.raw_response)
        else:
            return response
    
    
    def re_send_as_singles(self):
        for item in self.items:
            item.send(self.server, port=self.port)
        
        
    def raise_for_failure(self):
        if self.failed == self.total:
            raise ZabbixTotalSendError(self)
        if self.failed > 0:
            raise ZabbixPartialSendError(self)
        
        
    def __repr__(self, *args, **kwargs):
        return self.__str__()
    
    
    def __str__(self):
        if self.failed:
            if len(self.items) == 1:
                return '{} {}'.format(self.raw_response, self.items)
        return self.raw_response
    

class Item(object):
    def __init__(self, host, key, value, clock = None):
        self.host = host
        self.key = key
        self.value = value
        self.clock = get_clock(clock)
        
    
    def send(self,server, port=10051):
        item_dicts = [self.asdict()]
        packet = get_packet(item_dicts)
        result = send(packet, server, port)
        if result.failed:
            logger.error('item failed %s, %s',result, self.asdict())
            result.items.append(self)
        return result
    
    
    def asdict(self):
        return {
            'host': self.host,
            'key': self.key,
            'value': self.value,
            'clock' : self.clock
        }

        
class Items(object):
    def __init__(self,server='127.0.0.1', port=10051):
        self.server = server
        self.port = port
        self.items = []
    
    
    def add_item(self,item):
        self.items.append(item)
        return self
    
    
    def add_items(self, items):
        for item in items:
            self.add_item(item)
        return self
    
    
    @property
    def _send_batches(self):
        for i in xrange(0, len(self.items), MAX_ITEMS_PER_SEND):
            yield self.items[i:i + MAX_ITEMS_PER_SEND]
            
        
    def send(self):
        results = []
        for batch in self._send_batches:
            item_dicts = [item.asdict() for item in batch]
            packet = get_packet(item_dicts)
            result = send(packet, self.server, self.port)
            if result.failed:
                result.items = batch
                result.server = self.server
                result.port = self.port
            results.append(result)
        return results
        
    
class LLD(object):
    def __init__(self, host, key, rows, format_key=True, key_template='{#%s}'):
        self.host = host
        self.key = key
        self.clock = None
        self.rows = rows
        self.format_key = format_key
        self.key_template = key_template
        

    def add_row(self, **row_items):
        row = {}
        for k,v in row_items.items():
            if self.format_key:
                key = self.key_template % k
            else:
                key = k
            row[key] = v
        self.rows.append(row)
        self.clock = get_clock(None)
        return self
    
    
    def add_rows(self, list_of_dicts):
        for row in list_of_dicts:
            self.add_row(**row)
        return self
    
    
    def send(self, server, port=10051):
        item_dicts = [self.asdict()]
        packet = get_packet(item_dicts)
        result = send(packet, server, port)
        if result.failed:
            logger.error('sending failed %s %s', result, self.asdict())
            result.items.append(self)
            result.server = server
            result.port = port
        return result
    
    
    def asdict(self):
        return {
            'host': self.host,
            'key': self.key,
            'value': self._get_value(),
            'clock': get_clock(self.clock)}
        
    
    def _get_value(self):
        return json.dumps({'data': self.rows})
    
    
    def __str__(self):
        return '{}:{}'.format(self.host, self.key)
    
    
class Host(object):
    def __init__(self, server, host):
        self.server = server
        self.host = host
        self.items = Items(server=server)
        
    
    def add_item(self, key, value, clock=None):
        self.items.add_item(Item(self.host, key, value, clock))
        
        
    def send(self):
        return self.items.send()
                
