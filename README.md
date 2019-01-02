# RipePing-2-Zabbix
Script for sending measurement from Ripe Atlas to Zabbix server.

Used repository:

https://github.com/MattParr/simplezabbixsender

https://github.com/RIPE-NCC/ripe-atlas-cousteau

### Example of use for RIPE Atlas ping measurement

python3 getping.py -m MEASUREMENT_ID -n ZABBIX_HOST

### Zabbix configuration

Need to import RIPE_ping_template.xml to zabbix.

Add template to ZABBIX_HOST

Run cron eg. every 2 minutes for getping.py script with parameters.
