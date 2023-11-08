CREATE TABLE IF NOT EXISTS public.products
(
    product_id int PRIMARY KEY,
    product_name varchar(32),
    price decimal
);

CREATE TABLE IF NOT EXISTS public.shops
(
    shop_id int PRIMARY KEY,
    shop_name varchar(32)
);

CREATE TABLE IF NOT EXISTS public.plan
(
    product_id int,
    shop_name varchar(32),
    plan_cnt int,
    plan_date date
);

CREATE TABLE IF NOT EXISTS public.shop_dns
(
    date date,
    product_id int,
    sales_cnt int
);

CREATE TABLE IF NOT EXISTS public.shop_mvideo
(
    date date,
    product_id int,
    sales_cnt int
);

CREATE TABLE IF NOT EXISTS public.shop_sitilink
(
    date date,
    product_id int,
    sales_cnt int
);

CREATE TABLE IF NOT EXISTS public.promo
(
    shop_id int,
    promo_date date,
    product_id int,
    discount float
);

INSERT INTO public.products(product_id, product_name, price)
VALUES
(0, 'Испорченный телефон', 50),
(1, 'Сарафанное радио', 100),
(2, 'Патефон', 150);

INSERT INTO public.shops(shop_id, shop_name)
VALUES
(0, 'shop_dns'),
(1, 'shop_mvideo'),
(2, 'shop_sitilink');

INSERT INTO public.plan(product_id, shop_name, plan_cnt, plan_date)
VALUES
(0, 'shop_dns', 10, '2023-01-01'),
(1, 'shop_dns', 20, '2023-01-01'),
(2, 'shop_dns', 30, '2023-01-01'),
(0, 'shop_mvideo', 20, '2023-01-01'),
(1, 'shop_mvideo', 30, '2023-01-01'),
(2, 'shop_mvideo', 40, '2023-01-01'),
(0, 'shop_sitilink', 30, '2023-01-01'),
(1, 'shop_sitilink', 40, '2023-01-01'),
(2, 'shop_sitilink', 50, '2023-01-01');

INSERT INTO public.shop_dns(date, product_id, sales_cnt)
VALUES
('2023-01-01', 0, 20),
('2023-01-01', 1, 10),
('2023-01-01', 2, 15),
('2023-01-02', 0, 20),
('2023-01-02', 1, 10),
('2023-01-02', 2, 0);

INSERT INTO public.shop_mvideo(date, product_id, sales_cnt)
VALUES
('2023-01-01', 0, 10),
('2023-01-01', 1, 30),
('2023-01-01', 2, 40),
('2023-01-02', 0, 0),
('2023-01-02', 1, 0),
('2023-01-02', 2, 0);

INSERT INTO public.promo(shop_id, promo_date, product_id, discount)
VALUES
(0, '2023-01-01', 1, 0.1),
(0, '2023-01-02', 1, 0.2),
(0, '2023-01-01', 0, 0.5);

