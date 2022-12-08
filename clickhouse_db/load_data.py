from csv import DictReader
from datetime import datetime
from clickhouse_driver import Client
import time
import sys
import threading

if len(sys.argv) < 2:
        print('Please specify number of workers')
        exit()
else:
        workers = int(sys.argv[1])
        file = sys.argv[2]


def thread_helper(db_client, query_str, function):
        db_client.execute(query_str, function)

def iter_csv(filename, workers=1, partition_number=1):
        converters = {'Open': float, 'Date': lambda x: datetime.strptime(x, '%Y-%m-%d'), 'Close': float, 'High': float, 'Low': float, 'Adj_>
        metrics = {'total_values': 0, 'total_rows': 0}
        with open(filename, 'r') as f:
                reader = list(DictReader(f))
                total_start = time.time()
                total_rows = len(reader)
                partition = total_rows // workers
                for line in reader[(partition_number - 1) * partition : partition * partition_number]:
                        yield {k: (converters[k](v) if k in converters else v) for k, v in line.items()}

qs = 'INSERT INTO dbsys VALUES'
clients = [Client('localhost') for i in range(workers)]
clients[0].execute('DROP TABLE dbsys')
clients[0].execute('CREATE TABLE IF NOT EXISTS dbsys (Date DateTime, Open Float32, High Float32, Low Float32, Close Float32, Adj_Close Floa>

threads = [threading.Thread(target=thread_helper, args=(clients[i - 1], qs, iter_csv(file, workers=workers, partition_number=i))) for>

start = time.time()

for t in threads:
        t.start()
for t in threads:
        t.join()
end = time.time()

run_time = end - start
total_rows = clients[0].execute('SELECT COUNT(*) FROM default.dbsys')[0][0]
print('Rows written per second: {0}'.format(total_rows / run_time))
print('Total rows written: {0}'.format(total_rows))
print('Values written per second: {0}'.format(total_rows * 8 / run_time))
print('Total values written: {0}'.format(total_rows * 8))
