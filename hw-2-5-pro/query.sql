WITH shop_all_raw AS(
SELECT *, 0 AS shop_id FROM shop_dns
UNION
SELECT *, 1 AS shop_id FROM shop_mvideo
UNION
SELECT *, 2 AS shop_id FROM shop_sitilink),

shop_all AS(
SELECT
  sa."date", sa.product_id, sa.sales_cnt, sa.shop_id,
  date_trunc('month', sa."date") AS sales_period,
  CASE
    WHEN po.promo_date IS NOT NULL THEN 1
    ELSE 0
  END AS is_promo,
  CASE
    WHEN po.promo_date IS NOT NULL THEN sa.sales_cnt
    ELSE 0
  END AS promo_cnt,
  CASE
    WHEN po.promo_date IS NOT NULL THEN pr.price * (1 - po.discount)
    ELSE pr.price
  END AS net_price
FROM shop_all_raw AS sa
LEFT JOIN promo AS po
  ON sa.shop_id = po.shop_id
    AND sa.product_id = po.product_id
    AND sa."date" = po.promo_date
LEFT JOIN products AS pr
  ON sa.product_id = pr.product_id),

sales_sum AS(
SELECT 
  sa.sales_period,
  sa.product_id,
  sa.shop_id,
  SUM(sa.sales_cnt) AS sales_fact,
  AVG(sa.sales_cnt) AS "avg(sales/date)",
  MAX(sa.sales_cnt) AS max_sales,
  SUM(sa.is_promo) AS promo_len,
  SUM(sa.promo_cnt) AS promo_sales_cnt,
  SUM(sa.sales_cnt * sa.net_price) AS income_fact,
  SUM(sa.promo_cnt * sa.net_price) AS promo_income
FROM shop_all AS sa
GROUP BY sa.sales_period, sa.product_id, sa.shop_id)

SELECT
  TO_CHAR(pl.plan_date, 'YYYY-MM') AS year_month,
  pl.shop_name,
  pr.product_name, 
  ss.sales_fact, pl.plan_cnt AS sales_plan, 
  CASE
    WHEN pl.plan_cnt > 0 
      THEN to_char(100.0 * ss.sales_fact / pl.plan_cnt, '990D99%')
    ELSE '100.00%'
  END AS "sales_fact/sales_plan",
  ss.income_fact AS income_fact,
  pl.plan_cnt * pr.price AS income_plan,
  CASE
    WHEN pl.plan_cnt * pr.price > 0 
      THEN to_char(100.0 * ss.income_fact / (pl.plan_cnt * pr.price), '990D99%')
    ELSE '100.00%'
  END AS "income_fact/income_plan",

-- PRO columns
  ROUND(ss."avg(sales/date)", 1) AS "avg(sales/date)",
  ss.max_sales,
  sa."date" AS date_max_sales,
  sa.is_promo AS date_max_sales_is_promo,
  CASE
    WHEN ss.max_sales > 0 THEN ROUND("avg(sales/date)"/ss.max_sales, 2)
    ELSE 1.00
  END AS "avg(sales/date)/max_sales",
  ss.promo_len,
  ss.promo_sales_cnt,
  CASE
    WHEN ss.sales_fact > 0 THEN to_char(100.0 * ss.promo_sales_cnt / ss.sales_fact, '990D99%')
    ELSE '100.00%'
  END AS "promo_sales_cnt/fact_sales",
  ss.promo_income,
  CASE
    WHEN ss.income_fact > 0 THEN to_char(100.0 * ss.promo_income / ss.income_fact, '990D99%')
    ELSE '100.00%'
  END AS "promo_income/fact_income"

FROM sales_sum AS ss
RIGHT JOIN shops AS sh
  ON ss.shop_id = sh.shop_id
RIGHT JOIN plan AS pl
  ON ss.sales_period = pl.plan_date 
    AND ss.product_id = pl.product_id
    AND sh.shop_name = pl.shop_name
LEFT JOIN products AS pr
  ON pl.product_id = pr.product_id
LEFT JOIN shop_all AS sa
  ON ss.shop_id = sa.shop_id
    AND ss.product_id = sa.product_id
    AND ss.max_sales = sa.sales_cnt
    AND ss.sales_period = sa.sales_period
ORDER BY ss.sales_period, sh.shop_id, pl.product_id