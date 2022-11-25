## Use Miniconda

Create environment, install pip -r requirements.txt



## Generate Data:

Set Path ("D:\a_timeseries") to where to download at the code of options.set_preference("browser.download.dir", r"D:\a_timeseries")


python get_data.py  in generate_data folder.

## TimescaleDB:

### Install on AWS EC2


### Local Docker

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=123 timescale/timescaledb-ha:pg14-latest


## ClickhouseDB:

### Install on AWS EC2





## InfluxDB:

### Install on AWS EC2




