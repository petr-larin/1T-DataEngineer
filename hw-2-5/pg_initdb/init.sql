CREATE TABLE IF NOT EXISTS public.products
(
    product_id int PRIMARY KEY,
    product_name varchar(32),
    price decimal
);

INSERT INTO public.products(product_id, product_name, price)
VALUES
(0, 'Испорченный телефон', 100),
(1, 'Сарафанное радио', 125),
(2, 'Патефон', 150);

CREATE TABLE IF NOT EXISTS public.shops
(
    shop_id int PRIMARY KEY,
    shop_name varchar(32)
);

INSERT INTO public.shops(shop_id, shop_name)
VALUES
(0, 'shop_dns'),
(1, 'shop_mvideo'),
(2, 'shop_sitilink');

CREATE TABLE IF NOT EXISTS public.plan
(
    product_id int,
    shop_name varchar(32),
    plan_cnt int,
    plan_date date
);

INSERT INTO public.plan(product_id, shop_name, plan_cnt, plan_date)
VALUES
(0, 'shop_sitilink', 150, '2023-01-01'),
(1, 'shop_sitilink', 120, '2023-01-01'),
(2, 'shop_sitilink', 0, '2023-01-01'),
(0, 'shop_mvideo', 145, '2023-01-01'),
(1, 'shop_mvideo', 105, '2023-01-01'),
(2, 'shop_mvideo', 90, '2023-01-01'),
(0, 'shop_dns', 135, '2023-01-01'),
(1, 'shop_dns', 110, '2023-01-01'),
(2, 'shop_dns', 75, '2023-01-01'),
(0, 'shop_sitilink', 155, '2023-02-01'),
(1, 'shop_sitilink', 125, '2023-02-01'),
(2, 'shop_sitilink', 100, '2023-02-01'),
(0, 'shop_mvideo', 150, '2023-02-01'),
(1, 'shop_mvideo', 110, '2023-02-01'),
(2, 'shop_mvideo', 95, '2023-02-01');
--(0, 'shop_dns', 140, '2023-02-01'),
--(1, 'shop_dns', 115, '2023-02-01'),
--(2, 'shop_dns', 80, '2023-02-01'),
--(0, 'shop_sitilink', 160, '2023-03-01');

CREATE TABLE IF NOT EXISTS public.shop_dns
(
    date date,
    product_id int,
    sales_cnt int
);

INSERT INTO public.shop_dns(date, product_id, sales_cnt)
VALUES
('2023-01-10', 0, 5),
('2023-01-10', 1, 4),
('2023-01-10', 2, 3),
('2023-01-11', 0, 6),
('2023-01-11', 1, 10),
('2023-01-11', 2, 9),
('2023-02-03', 0, 10),
('2023-02-13', 1, 9),
('2023-02-13', 2, 8);

CREATE TABLE IF NOT EXISTS public.shop_mvideo
(
    date date,
    product_id int,
    sales_cnt int
);

INSERT INTO public.shop_mvideo(date, product_id, sales_cnt)
VALUES
('2023-01-10', 0, 8),
('2023-01-10', 1, 8),
('2023-01-10', 2, 5),
('2023-01-11', 0, 10),
('2023-01-11', 1, 11),
('2023-01-11', 2, 7),
('2023-02-03', 0, 12),
('2023-02-13', 1, 12),
('2023-02-13', 2, 8);

CREATE TABLE IF NOT EXISTS public.shop_sitilink
(
    date date,
    product_id int,
    sales_cnt int
);

INSERT INTO public.shop_sitilink(date, product_id, sales_cnt)
VALUES
('2023-01-10', 0, 8),
('2023-01-10', 1, 9),
('2023-01-10', 2, 4),
('2023-01-11', 0, 14),
('2023-01-11', 1, 13),
('2023-01-11', 2, 9);

