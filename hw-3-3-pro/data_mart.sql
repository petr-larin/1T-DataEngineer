-- LIMIT 1 в подзапросах нужен потому, что если они возвращают более одного значения, 
-- возникает ошибка - при задвоении данных, или когда минимум/максимум приходится на несколько дат.

(
WITH CTE AS(
SELECT
  MAX(rate) AS max_rate,
  MIN(rate) AS min_rate,
  AVG(rate) AS avg_rate,
  MAX(quote_date) AS last_day
FROM public.rubbtc)
SELECT
  'BTC' AS currency,
  (SELECT quote_date FROM public.rubbtc WHERE rate = (SELECT max_rate FROM CTE) LIMIT 1) AS max_rate_date,
  (SELECT quote_date FROM public.rubbtc WHERE rate = (SELECT min_rate FROM CTE) LIMIT 1) AS min_rate_date,
  (SELECT ROUND(max_rate::numeric,2) FROM CTE) AS max_rate,
  (SELECT ROUND(min_rate::numeric,2) FROM CTE) AS min_rate,
  (SELECT ROUND(avg_rate::numeric,2) FROM CTE) AS avg_rate,
  (SELECT ROUND(rate::numeric,2) FROM public.rubbtc WHERE quote_date = (SELECT last_day FROM CTE) LIMIT 1) AS last_day_rate
)
UNION ALL
(
WITH CTE AS(
SELECT
  MAX(rate) AS max_rate,
  MIN(rate) AS min_rate,
  AVG(rate) AS avg_rate,
  MAX(quote_date) AS last_day
FROM public.rubeur)
SELECT
  'EUR' AS currency,
  (SELECT quote_date FROM public.rubeur WHERE rate = (SELECT max_rate FROM CTE) LIMIT 1) AS max_rate_date,
  (SELECT quote_date FROM public.rubeur WHERE rate = (SELECT min_rate FROM CTE) LIMIT 1) AS min_rate_date,
  (SELECT ROUND(max_rate::numeric,2) FROM CTE) AS max_rate,
  (SELECT ROUND(min_rate::numeric,2) FROM CTE) AS min_rate,
  (SELECT ROUND(avg_rate::numeric,2) FROM CTE) AS avg_rate,
  (SELECT ROUND(rate::numeric,2) FROM public.rubeur WHERE quote_date = (SELECT last_day FROM CTE) LIMIT 1) AS last_day_rate
)
UNION ALL
(
WITH CTE AS(
SELECT
  MAX(rate) AS max_rate,
  MIN(rate) AS min_rate,
  AVG(rate) AS avg_rate,
  MAX(quote_date) AS last_day
FROM public.rubusd)
SELECT
  'USD' AS currency,
  (SELECT quote_date FROM public.rubusd WHERE rate = (SELECT max_rate FROM CTE) LIMIT 1) AS max_rate_date,
  (SELECT quote_date FROM public.rubusd WHERE rate = (SELECT min_rate FROM CTE) LIMIT 1) AS min_rate_date,
  (SELECT ROUND(max_rate::numeric,2) FROM CTE) AS max_rate,
  (SELECT ROUND(min_rate::numeric,2) FROM CTE) AS min_rate,
  (SELECT ROUND(avg_rate::numeric,2) FROM CTE) AS avg_rate,
  (SELECT ROUND(rate::numeric,2) FROM public.rubusd WHERE quote_date = (SELECT last_day FROM CTE) LIMIT 1) AS last_day_rate
)