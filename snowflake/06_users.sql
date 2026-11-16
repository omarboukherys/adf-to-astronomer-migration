-- Comptes de service authentifiés par paire de clés (pas de mot de passe).
-- Clés générées hors du dépôt :
--   openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out svc_xxx_key.p8
--   openssl pkey -in svc_xxx_key.p8 -pubout -out svc_xxx_key.pub
-- Coller ici le contenu du .pub sans les lignes BEGIN/END ni retours à la ligne.
-- Les clés privées (.p8) ne sont jamais versionnées.
USE ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS SVC_AIRFLOW
  TYPE = SERVICE
  DEFAULT_ROLE = AIRFLOW_ROLE
  DEFAULT_WAREHOUSE = ETL_WH
  RSA_PUBLIC_KEY = '<CLE_PUBLIQUE_SVC_AIRFLOW>';

GRANT ROLE AIRFLOW_ROLE TO USER SVC_AIRFLOW;

CREATE USER IF NOT EXISTS SVC_ADF
  TYPE = SERVICE
  DEFAULT_ROLE = ADF_LOADER
  DEFAULT_WAREHOUSE = ETL_WH
  RSA_PUBLIC_KEY = '<CLE_PUBLIQUE_SVC_ADF>';

GRANT ROLE ADF_LOADER TO USER SVC_ADF;

SHOW USERS LIKE 'SVC_%';
