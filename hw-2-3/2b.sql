/*
ЗАДАНИЕ:

Напишите SQL-запрос, который выполняет следующие действия:

1. Возвращает список клиентов (имя и фамилия) с наибольшей 
общей суммой заказов.

2. Для каждого клиента из пункта 1 выводит список его заказов 
(номер заказа и общая сумма) в порядке убывания общей суммы заказов.

Данный вариант решения предполагает, что отбирается клиент или
клиенты с наибольшей суммой *всех* своих заказов.
*/

WITH agg_orders AS
  (SELECT ord.customerid, SUM(ord.totalamount) AS sum_totalamount
  FROM orders AS ord
  GROUP BY ord.customerid),
  
  top_customers AS
  (SELECT DISTINCT aord.customerid
  FROM agg_orders AS aord
  WHERE aord.sum_totalamount = 
    (SELECT MAX(sum_totalamount) 
     FROM agg_orders))
	 
SELECT cust.firstname, cust.lastname, ord.orderid, ord.totalamount
FROM orders AS ord
JOIN customers AS cust
  ON ord.customerid = cust.customerid
WHERE ord.customerid IN
  (SELECT customerid
   FROM top_customers)
ORDER BY ord.customerid, ord.totalamount DESC