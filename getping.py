from simplezabbixsender import LLD,Items,Item
import argparse
from ripeatlascousteau import AtlasStatusCheckRequest

def data_probes( atlas_result, host, verbosity ):
    data_probes = []
    p_res = atlas_result.get('probes')
    for pid in p_res:
      last = p_res.get(pid).get('last')
      last_packet_loss = p_res.get(pid).get('last_packet_loss')
      alert = p_res.get(pid).get('alert')
      if verbosity:
        print("Probe id: %s\n" % ( pid ) )
        print("Last ping: %s\n" % ( last ) )
        print("Alert: %s\n" % ( alert ) )
        print("Last packet loss: %s\n" % ( last_packet_loss ))

      item1 = Item(host=host, key='probe.last['+str(pid)+']', value = last)
      item2 = Item(host=host, key='probe.alert['+str(pid)+']', value = str(alert))
      item3 = Item(host=host, key='probe.last_packet_loss['+str(pid)+']', value = last_packet_loss)
      data_probes.append(item1)
      data_probes.append(item2)
      data_probes.append(item3)

    return data_probes

def main(args):

    if args.nameofhost:
      host = args.nameofhost
    else:
      print('Please add zabbix cdn host')
      return 1

    if args.measurement:
      msm = args.measurement 
    else:
      print('Please add measurement id')
      return 1

    '''Last result from atlas'''
    is_success_result, result = AtlasStatusCheckRequest(msm_id=msm).create()

    lld_data = []
    probes = result.get('probes')
    for p in probes:
      probeid = p
      lld_data.append({ '{#CDN_LLD_KEY12}': str(probeid) })

    #print(lld_data)

    lld = LLD(host = host, key = 'my.cdn.lld_items', rows = lld_data)
    result_lld = lld.send(server='ZABBIX.SERVER.HOST', port=10051)
    print(result_lld)


    if is_success_result:
      data = data_probes( result, host, args.verbosity )
    else:
      print('Problem with RIPE Atlas API')
      return 1
    if args.verbosity:
      print(data)

    items = Items(server='ZABBIX.SERVER.HOST', port=10051)
    items.add_items(data)
    items.send()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v','--verbosity', action="count",default=0)
    parser.add_argument('-m','--measurement', help="measurement id with type ping")
    parser.add_argument('-n','--nameofhost', help="name of cdn host in zabbix")
    args = parser.parse_args()
    main( args )
