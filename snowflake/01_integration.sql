-- Storage integration : Snowflake lit le conteneur Azure Blob sans clé stockée.
-- Après création :
--   1. ouvrir AZURE_CONSENT_URL (DESC ci-dessous) et accepter le consentement ;
--   2. dans Azure, sur le compte de stockage, donner le rôle « Lecteur des données Blob du stockage »
--      à l'application AZURE_MULTI_TENANT_APP_NAME (partie avant le « _ »).
-- Le compte Snowflake est hébergé sur AWS (eu-west-3) : la lecture depuis Azure est inter-cloud.
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS AZURE_LANDING_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://stadfastroomar.blob.core.windows.net/landing/');

DESC STORAGE INTEGRATION AZURE_LANDING_INT;
