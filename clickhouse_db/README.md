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
