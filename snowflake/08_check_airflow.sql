-- Contrôles du chargement Airflow (schéma RAW_AIRFLOW) et comparaison avec ADF.
USE ROLE ACCOUNTADMIN;

-- Après le premier test (1er octobre) : 1200 / 80 / 183, fichier source rempli partout.
SELECT 'CUSTOMERS' AS t, COUNT(*) AS nb, COUNT(_source_file) AS avec_source, MIN(_source_file) AS fichier FROM RETAIL.RAW_AIRFLOW.CUSTOMERS
UNION ALL SELECT 'PRODUCTS', COUNT(*), COUNT(_source_file), MIN(_source_file) FROM RETAIL.RAW_AIRFLOW.PRODUCTS
UNION ALL SELECT 'ORDERS',   COUNT(*), COUNT(_source_file), MIN(_source_file) FROM RETAIL.RAW_AIRFLOW.ORDERS;

-- Après le rattrapage des 30 jours (résultat obtenu le 20/11/2026) :
--   CUSTOMERS  adf 2001  airflow 2001  avec_source 2001  fichiers 30
--   PRODUCTS   adf 96    airflow 96    avec_source 96    fichiers 10 (20 fichiers vides)
--   ORDERS     adf 11630 airflow 11630 avec_source 11630 fichiers 30
-- Le 1er octobre a été chargé deux fois (test puis rattrapage) sans doublon : chargement idempotent.
SELECT 'CUSTOMERS' AS t,
       (SELECT COUNT(*) FROM RETAIL.RAW_ADF.CUSTOMERS)                         AS adf,
       (SELECT COUNT(*) FROM RETAIL.RAW_AIRFLOW.CUSTOMERS)                     AS airflow,
       (SELECT COUNT(_source_file) FROM RETAIL.RAW_AIRFLOW.CUSTOMERS)          AS airflow_avec_source,
       (SELECT COUNT(DISTINCT _source_file) FROM RETAIL.RAW_AIRFLOW.CUSTOMERS) AS fichiers
UNION ALL SELECT 'PRODUCTS',
       (SELECT COUNT(*) FROM RETAIL.RAW_ADF.PRODUCTS),
       (SELECT COUNT(*) FROM RETAIL.RAW_AIRFLOW.PRODUCTS),
       (SELECT COUNT(_source_file) FROM RETAIL.RAW_AIRFLOW.PRODUCTS),
       (SELECT COUNT(DISTINCT _source_file) FROM RETAIL.RAW_AIRFLOW.PRODUCTS)
UNION ALL SELECT 'ORDERS',
       (SELECT COUNT(*) FROM RETAIL.RAW_ADF.ORDERS),
       (SELECT COUNT(*) FROM RETAIL.RAW_AIRFLOW.ORDERS),
       (SELECT COUNT(_source_file) FROM RETAIL.RAW_AIRFLOW.ORDERS),
       (SELECT COUNT(DISTINCT _source_file) FROM RETAIL.RAW_AIRFLOW.ORDERS);
