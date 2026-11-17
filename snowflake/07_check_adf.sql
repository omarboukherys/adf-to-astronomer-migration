-- Contrôles du chargement ADF (schéma RAW_ADF).
USE ROLE ACCOUNTADMIN;

-- Volumes : attendu après le rattrapage des 30 jours : 2001 / 96 / 11630.
-- avec_source = 0 : la copie directe ADF ne sait pas ajouter le nom du fichier source.
SELECT 'CUSTOMERS' AS t, COUNT(*) AS nb, COUNT(_source_file) AS avec_source FROM RETAIL.RAW_ADF.CUSTOMERS
UNION ALL SELECT 'PRODUCTS', COUNT(*), COUNT(_source_file) FROM RETAIL.RAW_ADF.PRODUCTS
UNION ALL SELECT 'ORDERS',   COUNT(*), COUNT(_source_file) FROM RETAIL.RAW_ADF.ORDERS;

-- Aucune ligne d'en-tête ne doit apparaître
SELECT * FROM RETAIL.RAW_ADF.ORDERS LIMIT 5;

-- Requêtes réellement envoyées par ADF (diagnostic)
SELECT start_time, query_text
FROM TABLE(RETAIL.INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(USER_NAME => 'SVC_ADF', RESULT_LIMIT => 100))
WHERE query_text ILIKE '%COPY INTO%'
ORDER BY start_time DESC
LIMIT 3;

SELECT start_time, query_text
FROM TABLE(RETAIL.INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(USER_NAME => 'SVC_ADF', RESULT_LIMIT => 100))
WHERE query_text ILIKE '%CREATE%STAGE%'
ORDER BY start_time DESC
LIMIT 1;

-- Repartir de zéro : ADF envoie FORCE = TRUE, toute relance recharge (et duplique) les fichiers.
-- TRUNCATE TABLE RETAIL.RAW_ADF.CUSTOMERS;
-- TRUNCATE TABLE RETAIL.RAW_ADF.PRODUCTS;
-- TRUNCATE TABLE RETAIL.RAW_ADF.ORDERS;
