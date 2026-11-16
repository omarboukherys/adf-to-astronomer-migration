-- Test manuel du chargement : un fichier avant et un après l'ajout de la colonne channel.
-- Résultat obtenu le 16/11/2026 : 183 lignes / 0 channel, puis 411 lignes / 411 channel.
USE ROLE ACCOUNTADMIN;

COPY INTO RETAIL.RAW_AIRFLOW.ORDERS
  FROM @RETAIL.LANDING.AZURE_LANDING/orders/
  FILES = ('orders_2026-10-01.csv', 'orders_2026-10-15.csv')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME);

SELECT _source_file, COUNT(*) AS nb_lignes, COUNT(channel) AS avec_channel
FROM RETAIL.RAW_AIRFLOW.ORDERS
GROUP BY 1 ORDER BY 1;

-- TRUNCATE (et non DELETE) efface aussi l'historique de chargement des fichiers :
-- sinon COPY INTO ignorerait ces deux fichiers pendant 64 jours.
TRUNCATE TABLE RETAIL.RAW_AIRFLOW.ORDERS;
SELECT COUNT(*) AS nb FROM RETAIL.RAW_AIRFLOW.ORDERS;
