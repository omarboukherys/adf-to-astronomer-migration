-- Tables brutes : toutes les colonnes en texte. Une valeur invalide (ex. 2026-13-45)
-- ne bloque pas le chargement ; dbt type et filtre ensuite.
-- _source_file et _loaded_at tracent l'origine de chaque ligne.
USE ROLE ACCOUNTADMIN;

CREATE TABLE IF NOT EXISTS RETAIL.RAW_AIRFLOW.CUSTOMERS (
  customer_id VARCHAR, city VARCHAR, segment VARCHAR, signup_date VARCHAR, updated_at VARCHAR,
  _source_file VARCHAR, _loaded_at TIMESTAMP_LTZ);

CREATE TABLE IF NOT EXISTS RETAIL.RAW_AIRFLOW.PRODUCTS (
  product_id VARCHAR, product_name VARCHAR, category VARCHAR, unit_price VARCHAR, updated_at VARCHAR,
  _source_file VARCHAR, _loaded_at TIMESTAMP_LTZ);

CREATE TABLE IF NOT EXISTS RETAIL.RAW_AIRFLOW.ORDERS (
  order_id VARCHAR, customer_id VARCHAR, product_id VARCHAR, quantity VARCHAR, unit_price VARCHAR,
  order_ts VARCHAR, status VARCHAR, updated_at VARCHAR, channel VARCHAR,
  _source_file VARCHAR, _loaded_at TIMESTAMP_LTZ);

-- Mêmes tables vides côté ADF, pour comparer les deux chargements
CREATE TABLE IF NOT EXISTS RETAIL.RAW_ADF.CUSTOMERS LIKE RETAIL.RAW_AIRFLOW.CUSTOMERS;
CREATE TABLE IF NOT EXISTS RETAIL.RAW_ADF.PRODUCTS  LIKE RETAIL.RAW_AIRFLOW.PRODUCTS;
CREATE TABLE IF NOT EXISTS RETAIL.RAW_ADF.ORDERS    LIKE RETAIL.RAW_AIRFLOW.ORDERS;

SHOW TABLES IN DATABASE RETAIL;
