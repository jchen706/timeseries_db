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
