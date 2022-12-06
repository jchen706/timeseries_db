## Use Miniconda

Create environment, install pip -r requirements.txt



## Generate Data:

Set Path ("D:\a_timeseries") to where to download at the code of options.set_preference("browser.download.dir", r"D:\a_timeseries")


python get_data.py  in generate_data folder.

## TimescaleDB:

## Set up EC2 Instance for client 

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html


sudo yum install python3-devel pip3 gcc


Install Miniconda on EC2


Create Conda Environment

Install the requirements.txt


## Set up EC2 instance for TimescaleDB

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


## timescaleDB settings changed

max_locks_per_transaction = 1024


### Local Docker

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=123 timescale/timescaledb-ha:pg14-latest


## ClickhouseDB:

### Install on AWS EC2

Use the AWS AMI

## InfluxDB:

### Install on AWS EC2

Use the AWS AMI





