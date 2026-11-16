-- Base, schémas et warehouse par défaut.
-- RAW_ADF et RAW_AIRFLOW reçoivent les mêmes fichiers : l'un par l'ancien pipeline ADF,
-- l'autre par le nouveau DAG Airflow, pour comparer les deux chargements.
USE ROLE ACCOUNTADMIN;

-- Coûts : le warehouse s'arrête après 60 s d'inactivité
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS RETAIL;
CREATE SCHEMA IF NOT EXISTS RETAIL.LANDING;      -- stage et formats de fichiers
CREATE SCHEMA IF NOT EXISTS RETAIL.RAW_ADF;      -- chargé par l'ancien pipeline ADF
CREATE SCHEMA IF NOT EXISTS RETAIL.RAW_AIRFLOW;  -- chargé par le nouveau DAG Airflow

SHOW SCHEMAS IN DATABASE RETAIL;
