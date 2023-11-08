-- 2. Промежуточные состояния:

-- Сначала вычислите средние продажи для каждой категории товаров 
-- в течение квартала с использованием -State комбинатора и функции avgState.
CREATE TABLE hw.avg_sales
(
  order_quarter UInt32,
  sub_category String,
  sales AggregateFunction(avg, Decimal(16, 2))
)
ENGINE = AggregatingMergeTree()
ORDER BY (order_quarter, sub_category);

CREATE MATERIALIZED VIEW hw.avg_sales_mv TO hw.avg_sales AS
SELECT 
    toRelativeQuarterNum(order_date) AS order_quarter, 
    sub_category,
    avgState(sales) AS sales
FROM hw.superstore 
GROUP BY order_quarter, sub_category
ORDER BY order_quarter, sub_category;

-- Теперь, чтобы материализованное представление заработало, надо удалить значения
-- из основной таблицы:
TRUNCATE hw.superstore;

-- и заново ее заполнить, для этого надо запустить команду
-- $ docker exec -it ch-2-8-pro sh /docker-entrypoint-initdb.d/2-read-csv.sh
-- Из Windows можно использовать скрипт docker-exec-read-csv.bat.

-- средние продажи для каждой категории товаров в течение квартала:
SELECT 
  concat(toString(intDiv(order_quarter, 4)), '-Q', toString(order_quarter % 4 + 1)) AS quarter, 
  sub_category, 
  toDecimal64(avgMerge(sales), 2) AS avg_sales_per_qtr_per_cat
FROM hw.avg_sales
GROUP BY order_quarter, sub_category
ORDER BY order_quarter, sub_category;

-- общая средняя продажа за квартал:
SELECT 
  concat(toString(intDiv(order_quarter, 4)), '-Q', toString(order_quarter % 4 + 1)) AS quarter, 
  toDecimal64(avgMerge(sales), 2) AS avg_sales_per_qtr 
FROM hw.avg_sales
GROUP BY order_quarter
ORDER BY order_quarter;