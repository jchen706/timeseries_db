Install InfluxDB

Using Ubuntu/Debian:
wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.5.1-xxx.deb
sudo dpkg -i influxdb2-2.5.1-xxx.deb


Start Influx service:
sudo service influxdb start

Restart service and ensure it is working: 
sudo service influxdb status

set up InfluxDB:
influx setup

---Input desired Username, Password, Organization Name, Bucket Name (table name)

Go to http://localhost:8086 
And add the following buckets:
'one_stock'
'five_stock'
'all_stock'
'mini1000'
'mini5000'
'mini10000'


Now run file in new terminal while Influxdb is running
in ./influx_db run:
python client.py 

