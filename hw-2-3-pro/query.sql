WITH rating AS
  (SELECT pr.customerid, AVG(rating) AS avg_rat
  FROM productreviews AS pr
  GROUP BY pr.customerid),

tot_prod_qty AS
  (SELECT o.customerid, SUM(od.quantity) AS prodqty
  FROM orders AS o
  LEFT JOIN orderdetails AS od
    ON o.orderid = od.orderid
  GROUP BY o.customerid)

SELECT c.customerid, c.firstname, c.lastname, 
  COALESCE(SUM(o.totalamount) OVER(PARTITION BY c.customerid),0) AS ordersum,
  COALESCE(q.prodqty, 0) AS prodqty,
  o.orderid,
  COALESCE(o.totalamount, 0) AS totalamount,
  CASE
    WHEN o.orderdate IS NULL THEN NULL
    WHEN o.orderdate < NOW() - interval '30d' THEN '-'
	ELSE '+'
  END AS New,
  r.avg_rat
FROM customers AS c
LEFT JOIN orders AS o
  ON c.customerid = o.customerid
LEFT JOIN rating AS r
  ON c.customerid = r.customerid
LEFT JOIN tot_prod_qty AS q
  ON c.customerid = q.customerid
ORDER BY c.customerid, o.orderdate DESC