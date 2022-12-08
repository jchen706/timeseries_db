from csv import DictReader
from datetime import datetime
from clickhouse_driver import Client
import time
import statistics
import threading
import sys

if not len(sys.argv) > 1:
        print('Please supply number of workers argument')
        exit()
print('NUMBER OF WORKERS: {0}'.format(sys.argv[1]))
workers = int(sys.argv[1])
client1 = Client('localhost')

def thread_helper(c, q):
        return c.execute(q)
		
def run_query(query, workload_num, query_num, num_threads=1):
        total_records = client1.execute('SELECT COUNT(*) FROM default.dbsys')[0][0]
        latencies = []
        if num_threads == 1:
                for i in range(5):
                        start = time.time()
                        client1.execute(query)
                        end = time.time()
                        latencies.append(end - start)
        elif num_threads == 2:
                client2 = Client('localhost')
                partition = total_records // 2
                for i in range(1, 3):
                        client1.execute('DROP TABLE IF EXISTS default.t{0}'.format(i))
                        client1.execute('CREATE TABLE default.t{0} as default.dbsys'.format(i))
                        client1.execute("""INSERT INTO default.t{0} SELECT Date, Open, High, Low, Close, Adj_Close, Volume, Symbol FROM (SELECT  *, ROW_NUMBER() OVER (ORDER BY Date) AS rn FROM default.dbsys) q WHERE rn > {1} and rn <= {2} ORDER BY Date""".format(i, partition * (i - 1), partition * i))
                for i in range(5):
                        t1 = threading.Thread(target=thread_helper, args=(client1, query.replace('dbsys', 't1'),))
						t2 = threading.Thread(target=thread_helper, args=(client2, query.replace('dbsys', 't2'),))
                        start = time.time()
                        t1.start()
                        t2.start()
                        t1.join()
                        t2.join()
                        end = time.time()
                        latencies.append(end - start)
        elif num_threads == 3:
                client2, client3 = Client('localhost'), Client('localhost')
                partition = total_records // 3
                for i in range(1, 4):
                        client1.execute('DROP TABLE IF EXISTS default.t{0}'.format(i))
                        client1.execute('CREATE TABLE default.t{0} as default.dbsys'.format(i))
                        client1.execute("""INSERT INTO default.t{0} SELECT Date, Open, High, Low, Close, Adj_Close, Volume, Symbol FROM (SELECT  *, ROW_NUMBER() OVER (ORDER BY Date) AS rn FROM default.dbsys) q WHERE rn > {1} and rn <= {2} ORDER BY Date""".format(i, partition * (i - 1), partition * i))
                for i in range(5):
						t1 = threading.Thread(target=thread_helper, args=(client1, query.replace('dbsys', 't1'),))
                        t2 = threading.Thread(target=thread_helper, args=(client2, query.replace('dbsys', 't2'),))
                        t3 = threading.Thread(target=thread_helper, args=(client3, query.replace('dbsys', 't3'),))
                        start = time.time()
                        t1.start()
                        t2.start()
                        t3.start()
                        t1.join()
                        t2.join()
                        t3.join()
                        end = time.time()
                        latencies.append(end - start)
        elif num_threads == 4:
                client2, client3, client4 = Client('localhost'), Client('localhost'), Client('localhost')
				partition = total_records // 4
                for i in range(1, 5):
                        client1.execute('DROP TABLE IF EXISTS default.t{0}'.format(i))
                        client1.execute('CREATE TABLE default.t{0} as default.dbsys'.format(i))
                        client1.execute("""INSERT INTO default.t{0} SELECT Date, Open, High, Low, Close, Adj_Close, Volume, Symbol FROM (SELECT  *, ROW_NUMBER() OVER (ORDER BY Date) AS rn FROM default.dbsys) q WHERE rn > {1} and rn <= {2} ORDER BY Date""".format(i, partition * (i - 1), partition * i))
                for i in range(5):
                        t1 = threading.Thread(target=thread_helper, args=(client1, query.replace('dbsys', 't1'),))
                        t2 = threading.Thread(target=thread_helper, args=(client2, query.replace('dbsys', 't2'),))
                        t3 = threading.Thread(target=thread_helper, args=(client3, query.replace('dbsys', 't3'),))
                        t4 = threading.Thread(target=thread_helper, args=(client4, query.replace('dbsys', 't4'),))
                        start = time.time()
                        t1.start()
                        t2.start()
                        t3.start()
                        t4.start()
                        t1.join()
                        t2.join()
                        t3.join()
						t4.join()
                        end = time.time()
                        latencies.append(end - start)
        print("Min latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, min(latencies)))
        print("Max latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, max(latencies)))
        print("Median latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, statistics.median(latencies)))
        print("Mean latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, statistics.mean(latencies)))
        print("Standard Deviation latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, statistics.stdev(latencies)))

print('~~~~~~~~~WORKLOAD 2~~~~~~~~~~')
print('~~~~~~~~~QUERY 1~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), MAX(Close) FROM default.dbsys WHERE Date > toDateTime('2013-12-01') AND Date < toDateTime('2014-01-01') GROUP BY Date""", 2, 1, workers)

print('~~~~~~~~~QUERY 2~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), MAX(Close) FROM default.dbsys WHERE Date > toDateTime('2013-01-01') AND Date < toDateTime('2014-01-01') GROUP BY Date""", 2, 2, workers)

print('~~~~~~~~~QUERY 3~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), MAX(Close) FROM default.dbsys WHERE Date > toDateTime('2013-12-01') AND Date < toDateTime('2014-01-01') GROUP BY Date, Symbol""", 2, 3, workers)

print('~~~~~~~~~QUERY 4~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), MAX(Close) FROM default.dbsys WHERE Date > toDateTime('2013-01-01') AND Date < toDateTime('2014-01-01') GROUP BY Date, Symbol""", 2, 4, workers)

print('~~~~~~~~~QUERY 5~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), MAX(Close) FROM default.dbsys WHERE Date > toDateTime('2013-01-01') AND Date < toDateTime('2013-01-07') GROUP BY Date, Symbol""", 2, 5, workers)

print('~~~~~~~~~QUERY 6~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), AVG(Close) FROM default.dbsys WHERE Date > toDateTime('2013-12-01') AND Date < toDateTime('2014-01-01') GROUP BY Date""", 2, 6, workers)

print('~~~~~~~~~QUERY 7~~~~~~~~~~')
run_query("""SELECT toStartOfWeek(Date), AVG(Close) FROM default.dbsys WHERE Date > toDateTime('2013-12-01') AND Date < toDateTime('2014-01-01') GROUP BY Date, Symbol""", 2, 7, workers)

print('~~~~~~~~~WORKLOAD 3~~~~~~~~~~')
run_query("""Select * from default.dbsys as t1 join (SELECT Date, Symbol, AVG(Close) OVER(ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ten_day_avg_close FROM default.dbsys WHERE Date > NOW() - INTERVAL '1 month' ORDER BY Date DESC) as t2 on t1.Date=t2.Date and t1.Symbol=t2.Symbol where Close > ten_day_avg_close""", 3, 1, workers)

print('~~~~~~~~~WORKLOAD 4~~~~~~~~~~')
run_query("""select current - value_prev as change from (SELECT Close as current, min(Close) over (order by Close rows between 1 preceding and 1 preceding) as value_prev FROM default.dbsys) where change > 0""", 4, 1, workers)
