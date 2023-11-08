WITH xorders AS
(SELECT c.customerid, c.firstname, c.lastname, o.orderid, 
  COALESCE(o.totalamount, 0) AS totalamount,
  COALESCE(SUM(o.totalamount) OVER(PARTITION BY c.customerid),0) AS ordersum
FROM customers AS c
LEFT JOIN orders AS o
  ON c.customerid = o.customerid)

SELECT x.firstname, x.lastname, x.orderid, x.totalamount
FROM xorders AS x
WHERE x.ordersum >
  (SELECT AVG(ordersum) FROM
    (SELECT DISTINCT x.customerid, x.ordersum
    FROM xorders AS x))
ORDER BY x.customerid, x.totalamount DESC