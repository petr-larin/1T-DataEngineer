--BEGIN;

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
    CustomerID int NOT NULL,
    OrderDate timestamp,
    TotalAmount decimal
);

CREATE TABLE IF NOT EXISTS public.OrderDetails
(
    OrderDetailID int PRIMARY KEY,
    OrderID int NOT NULL,
    ProductID int NOT NULL,
    Quantity int,
    UnitPrice decimal
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
  (0, 1, '2023-09-05 00:10:20', 100.00),
  (1, 0, '2023-09-08 12:17:05', 75.00),
  (2, 2, '2023-09-11 18:38:15', 50.00),
  (3, 3, '2023-09-05 19:19:00', 100.00),
  (4, 1, '2023-09-04 19:25:39', 100.00),
  (5, 1, '2023-09-15 07:50:45', 40.00),
  (6, 3, '2023-09-16 09:05:09', 35.00),
  (7, 0, '2023-09-17 10:10:17', 85.00),
  (8, 0, '2023-09-17 11:58:36', 80.00);

--END;