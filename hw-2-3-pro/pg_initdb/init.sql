CREATE TABLE IF NOT EXISTS public.Customers
(
    CustomerID int PRIMARY KEY,
    FirstName varchar(32),
    LastName varchar(32) NOT NULL,
    Email varchar(32)
);

CREATE TABLE IF NOT EXISTS public.Orders
(
    OrderID int PRIMARY KEY,
    CustomerID int NOT NULL REFERENCES Customers(CustomerID),
    OrderDate timestamp,
    TotalAmount decimal
);

CREATE TABLE IF NOT EXISTS public.Products
(
    ProductID int PRIMARY KEY,
    ProductName varchar(32),
    CategoryID int NOT NULL,
    Price decimal
);

CREATE TABLE IF NOT EXISTS public.OrderDetails
(
    OrderDetailID int PRIMARY KEY,
    OrderID int NOT NULL REFERENCES Orders(OrderID),
    ProductID int NOT NULL REFERENCES Products(ProductID),
    Quantity int,
    UnitPrice decimal
);

CREATE TABLE IF NOT EXISTS public.ProductReviews
(
    ReviewID int PRIMARY KEY,
    ProductID int NOT NULL REFERENCES Products(ProductID),
    CustomerID int NOT NULL REFERENCES Customers(CustomerID),
    Rating int, -- 1 to 5
    ReviewText text
);

INSERT INTO public.Customers (CustomerID, FirstName, LastName, Email)
VALUES
  (0, 'Alexa','Martinez','suspendisse@icloud.ca'),
  (1, 'Paul','Scott','at.augue@protonmail.com'),
  (2, 'Chandler','Mcpherson','tincidunt.orci@icloud.com'),
  (3, 'Acton','Macias','nostra@google.net'),
  (4, 'Steven','Sharp','faucibus.leo@yahoo.edu');

INSERT INTO public.Orders (OrderID, CustomerID, OrderDate, TotalAmount)
VALUES
  (0, 1, '2023-08-05 00:10:20', 100.00),
  (1, 0, '2023-08-08 12:17:05', 75.00),
  (2, 2, '2023-08-11 18:38:15', 50.00),
  (3, 3, '2023-08-17 19:19:00', 100.00),
  (4, 1, '2023-08-19 19:25:39', 100.00),
  (5, 1, '2023-08-27 07:50:45', 40.00),
  (6, 3, '2023-09-01 09:05:09', 35.00),
  (7, 0, '2023-09-09 10:10:17', 85.00),
  (8, 0, '2023-09-17 11:58:36', 80.00);

INSERT INTO public.Products (ProductID, ProductName, CategoryID, Price)
VALUES
  (0, 'Болт',-1,5),
  (1, 'Гайка',-1,5),
  (2, 'Саморез',-1,5),
  (3, 'Набор отверток',-1,20),
  (4, 'Шуруповерт',-1,80),
  (5, 'Дрон',-1,200);

INSERT INTO public.OrderDetails (OrderDetailID, OrderID, ProductID, Quantity, UnitPrice)
VALUES
  (0, 0, 4, 1, 80),
  (1, 0, 3, 1, 20),
  (2, 1, 1, 15, 5),
  (3, 2, 3, 2, 20),
  (4, 2, 0, 2, 5),
  (5, 3, 4, 1, 80),
  (6, 3, 0, 4, 5),
  (7, 4, 2, 10, 5),
  (8, 4, 1, 5, 5),
  (9, 4, 0, 5, 5),
  (10, 5, 2, 4, 5),
  (11, 5, 1, 4, 5),
  (12, 6, 2, 7, 5),
  (13, 7, 3, 2, 20),
  (14, 7, 0, 9, 5),
  (15, 8, 3, 1, 20),
  (16, 8, 0, 4, 5),
  (17, 8, 1, 4, 5),
  (18, 8, 2, 4, 5);

INSERT INTO public.ProductReviews (ReviewID, ProductID, CustomerID, Rating, ReviewText)
VALUES
  (0, 0, 2, 5, 'Отличный болт!!!'),
  (1, 4, 1, 1, 'Шуруповерт сразу сломался('),
  (2, 1, 0, 4, 'Гайка как гайка'),
  (3, 0, 2, 4, 'Болт всем рекомендую'),
  (4, 2, 1, 5, 'Саморезы топчик'),
  (5, 3, 2, 5, 'Офигительный набор отверток'),
  (6, 0, 1, 3, 'Эти болты мне сразу не понравились'),
  (7, 2, 1, 2, 'Все саморезы погнулись пришлось выбросить');