WITH part AS(
SELECT DISTINCT
  toDate(order_date) as order_date,
  category,
  SUM(revenue) OVER (PARTITION BY category ORDER BY order_date) AS cumulative_revenue,
  COUNT(*) OVER (PARTITION BY category ORDER BY order_date) AS cumulative_orders,
  CASE 
    WHEN cumulative_orders != 0 THEN cumulative_revenue / cumulative_orders
    ELSE NULL
  END AS average_check
FROM hw.sales)
SELECT 
  *, 
  argMax(order_date, average_check) OVER (PARTITION BY category) AS max_avg_check_date,
  MAX(average_check) OVER (PARTITION BY category) AS max_avg_check_value
FROM part
ORDER BY category, order_date 