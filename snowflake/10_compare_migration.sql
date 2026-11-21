-- Preuve de migration : ADF (RAW_ADF) contre Airflow 3 (RAW_AIRFLOW + MARTS dbt).
-- Une seule requête, un seul tableau : la colonne statut doit valoir OK partout.
-- Voir docs/migration-mapping.md.
USE ROLE AIRFLOW_ROLE;      -- lit RAW_ADF (05_roles.sql) et possède les marts créés par dbt
USE WAREHOUSE ETL_WH;
USE DATABASE RETAIL;

WITH controles AS (
    -- Chargement brut : les deux orchestrateurs doivent charger exactement les fichiers sources
    SELECT '01 Lignes brutes customers' AS controle,
           (SELECT COUNT(*) FROM RAW_ADF.CUSTOMERS)     AS adf,
           (SELECT COUNT(*) FROM RAW_AIRFLOW.CUSTOMERS) AS airflow,
           2001 AS attendu
    UNION ALL
    SELECT '02 Lignes brutes products',
           (SELECT COUNT(*) FROM RAW_ADF.PRODUCTS),
           (SELECT COUNT(*) FROM RAW_AIRFLOW.PRODUCTS),
           96
    UNION ALL
    SELECT '03 Lignes brutes orders',
           (SELECT COUNT(*) FROM RAW_ADF.ORDERS),
           (SELECT COUNT(*) FROM RAW_AIRFLOW.ORDERS),
           11630
    UNION ALL
    -- Traçabilité : ADF ne remplit pas _source_file (copie directe), Airflow le remplit partout
    SELECT '04 Lignes avec fichier source',
           (SELECT COUNT(_source_file) FROM RAW_ADF.CUSTOMERS)
         + (SELECT COUNT(_source_file) FROM RAW_ADF.PRODUCTS)
         + (SELECT COUNT(_source_file) FROM RAW_ADF.ORDERS),
           (SELECT COUNT(_source_file) FROM RAW_AIRFLOW.CUSTOMERS)
         + (SELECT COUNT(_source_file) FROM RAW_AIRFLOW.PRODUCTS)
         + (SELECT COUNT(_source_file) FROM RAW_AIRFLOW.ORDERS),
           13727
    UNION ALL
    -- 30 + 10 + 30 : les 20 fichiers produits vides n'apportent aucune ligne
    SELECT '05 Fichiers sources tracés',
           (SELECT COUNT(DISTINCT _source_file) FROM RAW_ADF.CUSTOMERS)
         + (SELECT COUNT(DISTINCT _source_file) FROM RAW_ADF.PRODUCTS)
         + (SELECT COUNT(DISTINCT _source_file) FROM RAW_ADF.ORDERS),
           (SELECT COUNT(DISTINCT _source_file) FROM RAW_AIRFLOW.CUSTOMERS)
         + (SELECT COUNT(DISTINCT _source_file) FROM RAW_AIRFLOW.PRODUCTS)
         + (SELECT COUNT(DISTINCT _source_file) FROM RAW_AIRFLOW.ORDERS),
           70
    UNION ALL
    -- Transformations dbt : uniquement côté Airflow (ADF n'en faisait aucune)
    SELECT '06 Clients (DIM_CUSTOMERS)',            NULL, (SELECT COUNT(*) FROM MARTS.DIM_CUSTOMERS), 1831
    UNION ALL
    SELECT '07 Produits (DIM_PRODUCTS)',            NULL, (SELECT COUNT(*) FROM MARTS.DIM_PRODUCTS), 80
    UNION ALL
    SELECT '08 Commandes valides (FCT_ORDERS)',     NULL, (SELECT COUNT(*) FROM MARTS.FCT_ORDERS), 6071
    UNION ALL
    SELECT '09 Commandes rejetées (AUDIT)',         NULL, (SELECT COUNT(*) FROM MARTS.AUDIT_REJECTED_ORDERS), 5
    UNION ALL
    SELECT '10 Chiffre d''affaires livré',          NULL,
           (SELECT SUM(amount) FROM MARTS.FCT_ORDERS WHERE status = 'DELIVERED'), 838711.64
    UNION ALL
    -- Commandes dont le client n'existe pas : signalées par le test dbt relationships (warn)
    SELECT '11 Commandes orphelines',               NULL,
           (SELECT COUNT(*) FROM MARTS.FCT_ORDERS f
             WHERE NOT EXISTS (SELECT 1 FROM MARTS.DIM_CUSTOMERS c WHERE c.customer_id = f.customer_id)),
           5
)
SELECT controle,
       adf,
       airflow,
       attendu,
       IFF(airflow = attendu, 'OK', 'ECART') AS statut
FROM controles
ORDER BY controle;
