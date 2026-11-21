-- Contrôles des marts produits par dbt (DAG dbt_retail, Cosmos).
-- Snowsight n'affiche (et n'exporte) que le résultat de la DERNIÈRE requête d'un « Run All » :
-- pour voir une requête précise, cliquer dedans puis Ctrl+Entrée.
USE ROLE AIRFLOW_ROLE;
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL;

-- 1. Statuts et montants des commandes valides
-- Résultat du 20/11/2026 (premier run de dbt_retail) :
--   CANCELLED 963 / 200 297,18 · DELIVERED 3 605 / 838 711,64
--   PLACED    589 / 139 818,83 · RETURNED   914 / 203 757,73   (total 6 071)
SELECT status, COUNT(*) AS nb, SUM(amount) AS total_amount
FROM MARTS.FCT_ORDERS
GROUP BY status
ORDER BY status;

-- 2. Volumes des marts sur une seule ligne
-- Résultat du 21/11/2026 : 1 831 · 80 · 6 071 · 5 (identique aux attendus du générateur)
SELECT
  (SELECT COUNT(*) FROM MARTS.DIM_CUSTOMERS)         AS dim_customers,
  (SELECT COUNT(*) FROM MARTS.DIM_PRODUCTS)          AS dim_products,
  (SELECT COUNT(*) FROM MARTS.FCT_ORDERS)            AS fct_orders,
  (SELECT COUNT(*) FROM MARTS.AUDIT_REJECTED_ORDERS) AS audit_rejected_orders;
