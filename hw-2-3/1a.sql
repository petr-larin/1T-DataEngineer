/*
ЗАДАНИЕ:

Напишите SQL-запрос, который выполняет следующие действия:

1. Возвращает список клиентов (имя и фамилия) с наибольшей 
общей суммой заказов.

Данный вариант решения предполагает, что отбирается клиент или
клиенты с наибольшей суммой *какого-либо отдельно взятого* заказа.
*/

SELECT cust.firstname, cust.lastname
FROM customers AS cust
WHERE cust.customerid IN
  (SELECT DISTINCT ord.customerid
  FROM orders AS ord
  WHERE ord.totalamount = 
    (SELECT MAX(totalamount) 
     FROM orders))

/*
Более короткий вариант, который может работать некорректно, если
среди клиентов есть полные тезки:

SELECT DISTINCT cust.firstname, cust.lastname
FROM orders AS ord
JOIN customers AS cust
ON ord.customerid = cust.customerid
WHERE ord.totalamount = 
  (SELECT MAX(totalamount) 
   FROM orders)
*/
