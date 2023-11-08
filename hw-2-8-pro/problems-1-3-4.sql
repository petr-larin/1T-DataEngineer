-- 1. Агрегация данных:

-- Вычислите общую сумму продаж по каждой категории товаров за последний месяц.
SELECT
  sub_category,
  SUM(sales) AS sales_by_sub_category
FROM hw.superstore
WHERE order_date >= '2017-12-01' -- датасет заканчивается на дате 2017-12-30
GROUP BY sub_category
ORDER BY sub_category

-- Определите средний чек покупателей за последний год.
SELECT
  customer_name, 
  SUM(sales)/COUNT(DISTINCT order_id) AS avg_check
FROM hw.superstore
WHERE order_date >= '2017-01-01' -- датасет заканчивается на дате 2017-12-30
GROUP BY customer_name
ORDER BY customer_name

-- 3. Работа с окнами данных:

-- Определите топ-5 товаров с наибольшей выручкой за последние 7 дней.
SELECT DISTINCT
  product_id,
  product_name,
  SUM(sales) OVER (PARTITION BY product_name) AS sales_sum
FROM hw.superstore
WHERE order_date >= '2017-12-25' -- датасет заканчивается на дате 2017-12-30
ORDER BY sales_sum DESC
LIMIT 5

-- то же самое, без оконной функции
SELECT
  product_id,
  product_name,
  SUM(sales) AS sales_sum
FROM hw.superstore
WHERE order_date >= '2017-12-25' -- датасет заканчивается на дате 2017-12-30
GROUP BY product_name, product_id 
ORDER BY sales_sum DESC
LIMIT 5

-- Найдите кумулятивную сумму продаж для каждой категории товаров за последние 3 месяца.
SELECT DISTINCT 
  order_date,
  sub_category,
  SUM(sales) OVER (PARTITION BY sub_category ORDER BY order_date) AS running_total
FROM hw.superstore
WHERE order_date >= '2017-10-01' -- датасет заканчивается на дате 2017-12-30
ORDER BY sub_category, order_date

-- 4. Оптимизация запросов:

-- Создайте отчет о продажах за последний год и сохраните его в отдельной таблице с 
-- использованием оператора INSERT.

-- Предположим, текущая дата - 15.12.2017.
-- Создадим отчет о продажах по категориям за 11 месяцев 2017:
CREATE TABLE IF NOT EXISTS hw.report_2017_11
(
    order_month UInt16,
    sub_category String,
    sales Decimal(16, 2)
)
ENGINE = MergeTree()
ORDER BY (order_month, sub_category);

INSERT INTO hw.report_2017_11
SELECT
  toMonth(order_date) AS order_month,
  sub_category,
  SUM(sales) AS sales_by_month
FROM hw.superstore
WHERE order_date BETWEEN '2017-01-01' AND '2017-11-30'
GROUP BY order_month, sub_category
ORDER BY order_month, sub_category;

-- Затем выполните запрос, который объединяет данные из этой таблицы с текущими 
-- данными о продажах и вычисляет общую сумму продаж за последний год.
-- >>> Прошу прощения за SELECT * но в этой ситуации мне кажется, что он оправдан)

-- Текущие данные о продажах:
CREATE TABLE IF NOT EXISTS hw.current_sales AS hw.superstore;

INSERT INTO hw.current_sales
SELECT * FROM hw.superstore 
WHERE order_date BETWEEN '2017-12-01' AND '2017-12-15';

-- Объединение:
SELECT * FROM hw.report_2017_11
UNION ALL
SELECT
  toMonth(order_date) AS order_month,
  sub_category,
  SUM(sales) AS sales_by_month
FROM hw.current_sales
GROUP BY order_month, sub_category
ORDER BY order_month, sub_category;