WITH shop_all AS (
SELECT *, 0 AS shop_id FROM shop_dns
UNION
SELECT *, 1 AS shop_id FROM shop_mvideo
UNION
SELECT *, 2 AS shop_id FROM shop_sitilink),

sales_sum AS (
SELECT 
  date_trunc('month', "date") AS sales_period,
  product_id,
  SUM(sales_cnt) AS sales_fact,
  shop_id
FROM shop_all
GROUP BY date_trunc('month', "date"), product_id, shop_id)

SELECT
  TO_CHAR(pl.plan_date, 'YYYY-MM') AS sales_period,
  pl.shop_name,
  pr.product_name, 
  ss.sales_fact, pl.plan_cnt AS sales_plan, 
  CASE
    WHEN pl.plan_cnt = 0 THEN '100.00%'
    ELSE TO_CHAR(100.0 * ss.sales_fact / pl.plan_cnt, '990D99%')
  END AS "sales_fact/sales_plan",
  ss.sales_fact * pr.price AS income_fact,
  pl.plan_cnt * pr.price AS income_plan,
    CASE
    WHEN pl.plan_cnt = 0 THEN '100.00%'
    ELSE TO_CHAR(100.0 * ss.sales_fact / pl.plan_cnt, '990D99%')
  END AS "income_fact/income_plan"
FROM sales_sum AS ss
RIGHT JOIN shops AS sh
  ON ss.shop_id = sh.shop_id
RIGHT JOIN plan AS pl
  ON ss.sales_period = pl.plan_date 
    AND ss.product_id = pl.product_id
    AND sh.shop_name = pl.shop_name
LEFT JOIN products AS pr
  ON pl.product_id = pr.product_id
ORDER BY sales_period, sh.shop_id, pl.product_id