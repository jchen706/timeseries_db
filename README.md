## Use Miniconda

Create environment, install pip -r requirements.txt



## Generate Data:

Set Path ("D:\a_timeseries") to where to download at the code of options.set_preference("browser.download.dir", r"D:\a_timeseries")


python get_data.py  in generate_data folder.

## TimescaleDB:

### Set up EC2 Instance for client 

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html


sudo yum install python3-devel pip3 gcc


Install Miniconda on EC2


Create Conda Environment

Install the requirements.txt


### Set up EC2 instance for TimescaleDB

Use the AWS AMI for TimescaleDB EC2 instance

SSH in: ssh -i ~/.ssh/KEY-PAIR.pem ubuntu@HOSTNAME

Switch to the postgres user: sudo -u postgres -s

sudo -u postgres psql postgres

The config directory is /etc/postgresql/12/main/.

in postgresql.conf, change listen_addresses = '*'

in pg_hba.conf, add 'host all all "IP-Address of EC2 / 32" scram-sha-256'
in pg_hba.conf, add 'host all all "Your IP-Address / 32" scram-sha-256'


Run psql then ALTER USER postgres PASSWORD 'newPassword'; to set a password.

Run timescaledb-tune to tune for the VM.

Run pg_ctlcluster 14 main reload to reload 

Run sudo service postgresql restart to restart.

In AWS EC2: create a security group for PostgreSQL, allowing inbound port 5432 for the EC2


### timescaleDB settings changed

max_locks_per_transaction = 1024


### Local Docker

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=123 timescale/timescaledb-ha:pg14-latest


## ClickhouseDB:

### Install ClickHouse

Add ClickHouse repo
```bash
sudo bash -c "echo 'deb http://repo.yandex.ru/clickhouse/deb/stable/ main/' > /etc/apt/sources.list.d/clickhouse.list"
```
Add key and update repolist
```bash
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4    # optional
sudo apt-get update
```

Install binaries 
```bash
sudo apt-get install -y clickhouse-client clickhouse-server
```
More details on how to get started with ClickHouse is available [here](https://clickhouse.yandex/docs/en/getting_started/)


Ensure ClickHouse is running
```bash
sudo service clickhouse-server restart
```

### Install Python Dependencies

Install the clickhouse-driver python package
```bash
pip install clickhouse-driver
```

If you need to install pip on EC2, instructions can be found here:
https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/eb-cli3-install-linux.html


### Load Data
The data loading script requires arguments for number of workers to use and csv file to load. Due to column names requiring no spaces, ensure that the "Adj Close" column is reformatted as "Adj_Close" in your input csv. Output metrics are printed to the console.
```bash
python3 load_data_multithread.py [number of workers] [data csv]
```

### Run Workloads
All other workloads are contained in a single script. You must specify how many workers to use. Output metrics are printed to the console.
```bash
python3 workloads_2_3_4.py [number of workers]
```


## InfluxDB:

### Install on AWS EC2

Use the AWS AMI

### In EC 2 Instance, install InfluxDB

wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.5.1-xxx.deb

sudo dpkg -i influxdb2-2.5.1-xxx.deb


### Start Influx service:
sudo service influxdb start

### estart service and ensure it is working: 
sudo service influxdb status

### set up InfluxDB:
influx setup

## Input desired Username, Password, Organization Name, Bucket Name (table name)

Go to http://localhost:8086 

And create the following buckets:
'one_stock'
'five_stock'
'all_stock'
'mini1000'
'mini5000'
'mini10000'


## Now run file in new terminal while Influxdb is running
## in ../influx_db run:
python client.py 


All workloads are included in the script, the metrics are saved to three csv files in the ./influx_db directory.




