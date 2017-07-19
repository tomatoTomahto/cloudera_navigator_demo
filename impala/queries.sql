/* Load raw data into external tables */
CREATE EXTERNAL TABLE IF NOT EXISTS ext_customers (
        cust_id INT, 
        cust_name STRING,
        cust_age INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/sgupta/customers';

CREATE EXTERNAL TABLE IF NOT EXISTS ext_products (
        prod_id INT, 
        prod_name STRING,
        prod_price FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/sgupta/products';

CREATE EXTERNAL TABLE IF NOT EXISTS ext_transactions (
        cust_id INT, 
        prod_id INT,
        qty INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/sgupta/transactions';

SELECT * FROM ext_customers;
SELECT * FROM ext_products;
SELECT * FROM ext_transactions;

/* Transform raw data into optimal parquet format */
CREATE TABLE IF NOT EXISTS customers (
        cust_id INT, 
        cust_name STRING,
        cust_age INT)
STORED AS PARQUET;
INSERT INTO customers SELECT * FROM ext_customers;

CREATE TABLE IF NOT EXISTS products (
        prod_id INT, 
        prod_name STRING,
        prod_price FLOAT)
STORED AS PARQUET;
INSERT INTO products SELECT * FROM ext_products;

CREATE TABLE IF NOT EXISTS transactions (
        cust_id INT, 
        prod_id INT,
        qty INT)
STORED AS PARQUET;
INSERT INTO transactions SELECT * FROM ext_transactions;

SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM transactions;

/* Join data into a view of customer transactions */
CREATE TABLE IF NOT EXISTS customer_transactions (
        cust_name STRING,
        prod_name STRING,
        total_cost FLOAT)
STORED AS PARQUET;

INSERT INTO customer_transactions
SELECT c.cust_name, p.prod_name, cast(t.qty*p.prod_price as float) as total_cost
FROM customers c, products p, transactions t 
WHERE c.cust_id = t.cust_id AND p.prod_id = t.prod_id;

CREATE TABLE IF NOT EXISTS customer_transactions_ext
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/sgupta/customer_transactions'
AS SELECT * FROM customer_transactions
