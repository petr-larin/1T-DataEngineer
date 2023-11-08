/*
ЗАДАНИЕ:

Напишите SQL-запрос, который выполняет следующие действия:

1. Возвращает список клиентов (имя и фамилия) с наибольшей 
общей суммой заказов.

Данный вариант решения предполагает, что отбирается клиент или
клиенты с наибольшей суммой *всех* своих заказов.
*/

WITH agg_orders AS
  (SELECT ord.customerid, SUM(ord.totalamount) AS sum_totalamount
  FROM orders AS ord
  GROUP BY ord.customerid)
	 
SELECT cust.firstname, cust.lastname
FROM customers AS cust
WHERE cust.customerid IN
  (SELECT DISTINCT ord.customerid
    FROM agg_orders AS ord
    WHERE ord.sum_totalamount = 
      (SELECT MAX(sum_totalamount) 
       FROM agg_orders))