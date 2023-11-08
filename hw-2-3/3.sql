/*
ЗАДАНИЕ:

Напишите SQL-запрос, который выполняет следующие действия:

3. Выводит только тех клиентов, у которых общая сумма заказов 
превышает среднюю общую сумму заказов всех клиентов.
*/

WITH agg_orders AS
  (SELECT ord.customerid, SUM(ord.totalamount) AS sum_totalamount
  FROM orders AS ord
  GROUP BY ord.customerid),
  
  top_customers AS
  (SELECT DISTINCT aord.customerid
  FROM agg_orders AS aord
  WHERE aord.sum_totalamount > 
   
    (SELECT AVG(sum_totalamount) -- это если не учитываем клиентов 
    FROM agg_orders))            -- без заказов
    /*
    либо, если учитываем клиентов без заказов:
    ((SELECT SUM(sum_totalamount)
     FROM agg_orders)) /
     (SELECT COUNT(*)
     FROM customers))
	*/
	 
SELECT cust.firstname, cust.lastname, ord.orderid, ord.totalamount
FROM orders AS ord
JOIN customers AS cust
  ON ord.customerid = cust.customerid
WHERE ord.customerid IN
  (SELECT customerid
   FROM top_customers)
ORDER BY ord.customerid, ord.totalamount DESC