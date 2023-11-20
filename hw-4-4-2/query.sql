-------------------------------
-- organizations

CREATE EXTERNAL TABLE IF NOT EXISTS organizations_x(
  ind INT,
  org_id STRING,
  org_name STRING,
  website STRING,
  country STRING,
  descr STRING,
  founded SMALLINT,
  industry STRING,
  n_empl INT,
  bucket SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/peter/db/org/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TABLE IF NOT EXISTS organizations(
  ind INT,
  org_id STRING,
  org_name STRING,
  website STRING,
  country STRING,
  descr STRING,
  founded SMALLINT,
  industry STRING,
  n_empl INT,
  bucket SMALLINT
)
CLUSTERED BY(bucket) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

FROM organizations_x
INSERT OVERWRITE TABLE organizations 
SELECT *;

-------------------------------
-- people

CREATE EXTERNAL TABLE IF NOT EXISTS people_x(
  ind INT,
  user_id STRING,
  first_name STRING,
  last_name STRING,
  sex STRING,
  email STRING,
  phone STRING,
  birth_date DATE,
  job_title STRING,
  bucket SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/peter/db/ppl/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TABLE IF NOT EXISTS people(
  ind INT,
  user_id STRING,
  first_name STRING,
  last_name STRING,
  sex STRING,
  email STRING,
  phone STRING,
  birth_date DATE,
  job_title STRING,
  bucket SMALLINT
)
CLUSTERED BY(bucket) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

FROM people_x
INSERT OVERWRITE TABLE people 
SELECT *;

-------------------------------
-- customers

CREATE EXTERNAL TABLE IF NOT EXISTS customers_x(
  ind INT,
  cust_id STRING,
  first_name STRING,
  last_name STRING,
  company STRING,
  city STRING,
  country STRING,
  phone1 STRING,
  phone2 STRING,
  email STRING,
  subs_date DATE,
  website STRING,
  bucket SMALLINT,
  part_year SMALLINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/user/peter/db/cust/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE TABLE IF NOT EXISTS customers(
  ind INT,
  cust_id STRING,
  first_name STRING,
  last_name STRING,
  company STRING,
  city STRING,
  country STRING,
  phone1 STRING,
  phone2 STRING,
  email STRING,
  subs_date DATE,
  website STRING,
  bucket SMALLINT
)
PARTITIONED BY(part_year SMALLINT)
CLUSTERED BY(bucket) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

FROM customers_x
INSERT OVERWRITE TABLE customers
PARTITION(part_year = 2020)
SELECT 
  ind, cust_id, first_name, last_name, company, city, country, phone1,
  phone2, email, subs_date, website, bucket
WHERE part_year = 2020;

FROM customers_x
INSERT OVERWRITE TABLE customers
PARTITION(part_year = 2021)
SELECT 
  ind, cust_id, first_name, last_name, company, city, country, phone1,
  phone2, email, subs_date, website, bucket
WHERE part_year = 2021;

FROM customers_x
INSERT OVERWRITE TABLE customers
PARTITION(part_year = 2022)
SELECT 
  ind, cust_id, first_name, last_name, company, city, country, phone1,
  phone2, email, subs_date, website, bucket
WHERE part_year = 2022;

-------------------------------
-- data mart

--EXPLAIN
WITH ages AS(
SELECT org.org_name AS company, 
  cust.part_year AS year,
  SUM(CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) <= 18 * 365
    THEN 1 ELSE 0
  END) AS age1,
  SUM(CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) > 18 * 365 AND
         DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) <= 25 * 365
    THEN 1 ELSE 0
  END) AS age2,
    SUM(CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) > 25 * 365 AND
         DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) <= 35 * 365
    THEN 1 ELSE 0
  END) AS age3,
    SUM(CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) > 35 * 365 AND
         DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) <= 45 * 365
    THEN 1 ELSE 0
  END) AS age4,
    SUM(CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) > 45 * 365 AND
         DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) <= 55 * 365
    THEN 1 ELSE 0
  END) AS age5,
  SUM(CASE
    WHEN DATEDIFF(CURRENT_DATE(), DATE(ppl.birth_date)) > 55 * 365
    THEN 1 ELSE 0
  END) AS age6
FROM organizations AS org
LEFT JOIN customers AS cust ON org.website = cust.website
JOIN people AS ppl ON ppl.ind = cust.ind
GROUP BY org.org_name, cust.part_year)

SELECT company, year, --greatest(age1, age2, age3, age4, age5, age6)
  CASE
    WHEN GREATEST(age1, age2, age3, age4, age5, age6) = age1 THEN '[0 - 18]'
    WHEN GREATEST(age1, age2, age3, age4, age5, age6) = age2 THEN '[19 - 25]'
    WHEN GREATEST(age1, age2, age3, age4, age5, age6) = age3 THEN '[26 - 35]'
    WHEN GREATEST(age1, age2, age3, age4, age5, age6) = age4 THEN '[36 - 45]'
    WHEN GREATEST(age1, age2, age3, age4, age5, age6) = age5 THEN '[46 - 55]'
    ELSE '[55+]'
  END AS age_group
FROM ages
ORDER BY company, year