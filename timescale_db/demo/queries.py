table = "stocks"
stock_paths = []
single_stock = 'ALLE'
five_stocks = "('A', 'ALLE', 'AAL','AAP','ABBV')"
stock_list = [
  'A',
  'ALLE',
  'AAL',
  'AAP',
  'ABBV',
  'ABC',
  'ACN',
  'ADBE',
  'ADI',
  'ADM'
]

# 1. Simple aggregate max for a stock, every week for 1 month
agg_max_stock_week_one_month = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where (time >= '2014-01-01') and (time < '2014-02-01') and symbol = '{}'
  group by bucket, symbol
  order by bucket, symbol
  """.format(table,single_stock)

# 2. Simple aggregate max for a stock, every week for 1 year
agg_max_stock_week_one_year = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where  (time >= '2014-01-01') and (time < '2015-01-01') and symbol = '{}'
  group by bucket, symbol
  order by bucket, symbol
  """.format(table,single_stock)

# 3. Simple aggregate max for five stock, every week for 1 month
agg_max_stock_week_five_one_month = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where (time >= '2014-01-01') and (time < '2014-02-01') and symbol IN {}
  group by bucket, symbol
  order by bucket, symbol
  """.format(table, five_stocks)

# 4. Simple aggregate max for five stock, every week for 1 year
agg_max_stock_week_five_one_year = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where (time >= '2014-01-01') and (time < '2015-01-01') and symbol IN {}
  group by bucket, symbol
  order by bucket, symbol
  """.format(table, five_stocks)

# Aggregate max across all stocks per week over 1 week
agg_max_stock_week_all_one_year = """
select time_bucket('7 day', time) as bucket, max(close), symbol
from stocks
where (time >= '2022-05-09') and (time <= '2022-05-13' )
group by bucket, symbol
order by bucket, symbol
""".format(table)


# Compute the average of a stock per week for 1 month
agg_avg_stock_week_one_month = """
select time_bucket('7 day', time) as bucket, avg(close),symbol
from {}
where (time >= '2014-01-01') and (time < '2014-02-01') and symbol = '{}'
group by bucket, symbol
order by bucket, symbol
""".format(table,single_stock)

# Compute the average of all stocks per week for 1 month
agg_avg_stock_week_all_one_month = """
select time_bucket('7 day', time) as bucket, avg(close),symbol
from {}
where (time >= '2014-01-01') and (time < '2014-02-01')
group by bucket, symbol
order by bucket, symbol
""".format(table)