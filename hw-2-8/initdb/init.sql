CREATE DATABASE IF NOT EXISTS hw;

CREATE TABLE IF NOT EXISTS hw.sales
(
    order_id UInt64,
    category UInt64,
    order_date DateTime,
    revenue Decimal(10,2)
)
ENGINE = MergeTree()
ORDER BY order_id;

INSERT INTO hw.sales 
VALUES
(1,0,'2023-01-01',10.00), (2,3,'2023-01-01',15.00), (3,3,'2023-01-01',25.00), (4,2,'2023-01-01',5.00), (5,0,'2023-01-01',10.00), (6,3,'2023-01-02',70.00), (7,1,'2023-01-02',100.00), (8,2,'2023-01-02',15.00), (9,3,'2023-01-02',30.00), (10,1,'2023-01-03',5.00), (11,2,'2023-01-03',10.00), (12,0,'2023-01-02',10.00);