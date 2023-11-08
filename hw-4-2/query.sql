DROP TABLE IF EXISTS public.sales;
DROP TABLE IF EXISTS public.products;

CREATE TABLE IF NOT EXISTS public.sales (
    sale_id SERIAL,
    sale_date date,
    customer_id int,
    product_id int,
    product_qty int
) DISTRIBUTED BY (sale_id)
  PARTITION BY RANGE (sale_date)
  (START (date '2023-01-01') INCLUSIVE
  END (date '2023-06-01') EXCLUSIVE
  EVERY (INTERVAL '1 month'));

CREATE TABLE IF NOT EXISTS public.products (
    product_id SERIAL PRIMARY KEY,
    product_price DECIMAL(10, 2)
) DISTRIBUTED BY (product_id);

ALTER TABLE public.sales 
  ADD CONSTRAINT fk FOREIGN KEY (product_id)
  REFERENCES public.products(product_id);

INSERT INTO public.products
  (product_price)
VALUES
(10.0),
(15.0),
(20.0),
(25.0);
 
INSERT INTO public.sales 
  (sale_date, customer_id, product_id, product_qty)
VALUES
('2023-01-05', 0, 0, 2),
('2023-01-10', 1, 1, 1),
('2023-01-20', 2, 0, 10),
('2023-01-25', 0, 1, 12),
('2023-02-10', 2, 2, 10),
('2023-02-15', 0, 3, 1),
('2023-02-20', 0, 2, 5),
('2023-03-05', 1, 0, 3),
('2023-03-10', 3, 2, 10),
('2023-03-25', 3, 1, 7),
('2023-04-05', 1, 3, 15),
('2023-04-05', 0, 2, 4),
('2023-04-15', 3, 0, 6),
('2023-04-30', 2, 0, 8),
('2023-05-30', 2, 0, 8);

SET OPTIMIZER = ON;

EXPLAIN
SELECT SUM(product_qty * product_price)
FROM public.sales AS s
JOIN public.products AS p
  ON s.product_id = p.product_id
WHERE s.product_id = 2
  AND '2023-02-01' <= s.sale_date 
  AND s.sale_date < '2023-04-01'






