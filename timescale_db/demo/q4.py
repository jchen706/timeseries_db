"""
select d.time, d.close, d.change_of_price, d.symbol
from  (
SELECT t.time, t.close, t.close - LAG(t.close, 1, t.close) OVER(partition by t.symbol ORDER BY t.symbol)
AS change_of_price, t.symbol
FROM (
select symbol, time, close 
from stocks 
order by symbol, time asc) 
as t 
) as d 
where change_of_price > 0
order by d.time desc

"""
