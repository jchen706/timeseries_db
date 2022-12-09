## Use Miniconda

Create environment, install 

```bash
pip -r requirements.txt
```


## Generate Data:

Set Path ("D:\a_timeseries") to where to download at the code of 
```bash
options.set_preference("browser.download.dir", r"D:\a_timeseries")
```
In the generate folder run:

```bash
python get_data.py 
```

## TimescaleDB:


### Set up EC2 instance for TimescaleDB

Use any of the AWS AMI for TimescaleDB EC2 instance, I used the Ubuntu 22, Timescale 2.8.
SSH into the EC2 instance on command line by following https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html.


The config directory is /etc/postgresql/12/main/.

```bash
cd /etc/postgresql/14/main/
```


in postgresql.conf, change "listen_addresses = '*'"

```bash
listen_addresses = '*'
```

in pg_hba.conf, add 'host all all "IP-Address of EC2 / 32" scram-sha-256'
in pg_hba.conf, add 'host all all "Your IP-Address / 32" scram-sha-256'
Certain Version of Ubuntu doesn't support scram-sha-256, so you have to change to md5.
Here is a tutorial: https://www.cybertec-postgresql.com/en/from-md5-to-scram-sha-256-in-postgresql/.

```bash
host all all 12.0.0.0/32 scram-sha-256
```

Switch to the postgres user: 

```bash
sudo -u postgres -s
```

Run psql then 
ALTER USER postgres PASSWORD 'newPassword'; to set a password.
You have to be in the postgres user in bash not the Ubuntu Root.

```bash
ALTER USER postgres PASSWORD '123';
```


Run timescaledb-tune to tune for the VM.

```bash
timescaledb-tune
```

Run sudo service postgresql restart to restart.

```bash
sudo service postgresql restart
```

In AWS EC2: create a security group for PostgreSQL, allowing inbound port 5432 for the EC2.
Here is a tutorial: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/authorizing-access-to-an-instance.html. 

This will log you into postgres user of PostgreSQL.
You may have enter the password.

```bash
sudo -u postgres psql postgres
```

### timescaleDB settings changed

max_locks_per_transaction = 1024

### Local Docker

```bash
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=123 timescale/timescaledb-ha:pg14-latest
```

For local docker, you'll have to push have the folder clone within docker to work with localhost.


### Set up EC2 Instance for client 

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html

```bash
sudo yum install python3-devel pip3 gcc
```

Install Miniconda or Anaconda on EC2 and Create Conda Environment. Here's is Anaconda:
https://medium.com/@GalarnykMichael/aws-ec2-part-3-installing-anaconda-on-ec2-linux-ubuntu-dbef0835818a.


Install the requirements.txt

```bash
pip -r requirements.txt
```

#### Run all the workloads , this will run all the workloads for timescaledb. May have to comment off workloads as the EC2 instance maybe timeout connection when connecting with another EC2 instance.


Change the password="123", host="18.204.21.200", in the ThreadedConnectionPool around line 140-143 as necessary.

To Run the Client: 

```bash
python3 client.py
```

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




