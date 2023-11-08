CREATE DATABASE IF NOT EXISTS hw;

CREATE TABLE IF NOT EXISTS hw.superstore
(
  row_id UInt64,
  order_id FixedString(15),
  order_date Date,
  ship_date Date,
  ship_mode String,
  customer_id FixedString(9),
  customer_name String,
  segment String,
  country String,
  city String,
  state String,
  postal_code UInt32,
  region String,
  product_id FixedString(16),
  category String,
  sub_category String,
  product_name String,
  sales Decimal(16, 2),
  quantity UInt32,
  discount Float32,
  profit Float32
)
ENGINE = MergeTree()
ORDER BY order_date;

