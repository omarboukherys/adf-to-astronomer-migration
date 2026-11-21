# Migration ADF → Airflow 3 (Astronomer) : correspondance et preuve

Ce document met en regard le pipeline Azure Data Factory « legacy »
(décrit dans [`adf-legacy.md`](adf-legacy.md)) et sa cible Airflow 3 sur Astronomer,
puis montre, chiffres à l'appui, que la cible charge les mêmes données
et va plus loin (traçabilité, idempotence, transformations dbt testées).

## Avant / après

```
AVANT (ADF)                                        APRÈS (Airflow 3 / Astronomer)

tr_daily_tumbling (24 h, 5 en parallèle)           load_landing_to_snowflake
  └── pl_load_daily_landing                          (CronDataIntervalTimetable, max_active_runs 5)
        └── ForEach_entity                             └── TaskGroup par entité
              └── Copy direct Blob → RAW_ADF                 wait_for_file (SqlSensor sur le stage)
                                                             └── load_file (DELETE + COPY INTO, 1 transaction)
                                                                   └── Asset retail.raw_airflow.<entité>
                                                                         │
                                                                         ▼
(aucune transformation)                            dbt_retail (Cosmos, déclenché par les 3 Assets)
                                                     staging (vues) → marts (tables) + tests dbt
```

## Correspondance des objets

| ADF (legacy) | Airflow 3 (cible) | Où |
|---|---|---|
| Déclencheur `tr_daily_tumbling` (fenêtre bascule 24 h, 01/10 → 31/10) | `CronDataIntervalTimetable("0 0 * * *", UTC)`, `start_date` 01/10, `end_date` 30/10 | `dags/load_landing_to_snowflake.py` |
| `@trigger().outputs.windowStartTime` | `{{ data_interval_start \| ds }}` | idem |
| `maxConcurrency: 5` | `max_active_runs=5` | idem |
| Rattrapage automatique des fenêtres passées | `catchup=False` + backfill Airflow 3 explicite (dry-run d'abord) | CLI / UI Airflow |
| Pipeline `pl_load_daily_landing` | DAG `load_landing_to_snowflake` | `dags/` |
| `ForEach_entity` (customers, products, orders) | Boucle Python + un `TaskGroup` par entité | idem |
| Activité Copy (copie directe) | `SqlSensor` (fichier présent ?) + `SQLExecuteQueryOperator` (DELETE + COPY INTO) | idem |
| Stratégie de l'activité : 2 nouvelles tentatives à 60 s | `retries=2`, `retry_delay=1 min` | idem |
| Service lié `ls_blob_landing` (identité managée) | Aucun : Airflow lit le Blob via le stage Snowflake `@RETAIL.LANDING.AZURE_LANDING` (storage integration) | `snowflake/01_integration.sql`, `02_stage.sql` |
| Service lié `ls_snowflake` (paire de clés `SVC_ADF`, rôle `ADF_LOADER`) | Connexion `snowflake_default` (paire de clés `SVC_AIRFLOW`, `SNOWFLAKE_JWT`, rôle `AIRFLOW_ROLE`) dans `.env`, hors dépôt | `.env` (non versionné) |
| Jeu de données `ds_landing_csv` (paramétré) | `FILES = ('<entité>_<date>.csv')` + format de fichier `CSV_HEADER` du stage | DAG + `snowflake/02_stage.sql` |
| Jeu de données `ds_snowflake_raw` → `RAW_ADF.<ENTITÉ>` | Tables `RAW_AIRFLOW.<ENTITÉ>` (même structure) | `snowflake/03_raw_tables.sql` |
| Supervision ADF (Monitor) | Interface Airflow (Grid, Logs, Assets) | UI Airflow |
| Export ARM (JSON) | Code Python versionné + tests `pytest` | `dags/`, `tests/` |
| — | DAG `dbt_retail` : dbt via Astronomer Cosmos, planifié sur les Assets | `dags/dbt_retail.py`, `include/dbt/retail/` |

## Résultats comparés

Mêmes 90 fichiers sources (30 jours × 3 entités), chargés par les deux orchestrateurs.

| Contrôle | ADF (legacy) | Airflow 3 (cible) | Attendu |
|---|---|---|---|
| Exécutions | 30 réussies | 30 réussies (backfill) | 30 |
| Lignes brutes customers / products / orders | 2 001 / 96 / 11 630 | 2 001 / 96 / 11 630 | 2 001 / 96 / 11 630 |
| Lignes avec fichier source | 0 % | 100 % | — |
| Fichiers sources distincts tracés | 0 | 70 (30 + 10 + 30 ; 20 fichiers produits vides) | 70 |
| Relance d'un jour déjà chargé | doublons (`FORCE = TRUE` sans nettoyage) | aucun doublon : le 01/10 chargé deux fois, totaux inchangés | — |
| Correspondance des colonnes | par position (`$1, $2…`) | par nom (`MATCH_BY_COLUMN_NAME`) | — |
| Secret Azure dans l'orchestrateur | identité managée de la fabrique | aucun | — |
| Transformation | aucune | dbt : 8 modèles, tests, 11 tâches en 54 s | — |
| Clients (`MARTS.DIM_CUSTOMERS`) | — | 1 831 | 1 831 |
| Produits (`MARTS.DIM_PRODUCTS`) | — | 80 | 80 |
| Commandes valides (`MARTS.FCT_ORDERS`) | — | 6 071 | 6 071 |
| Commandes rejetées (`MARTS.AUDIT_REJECTED_ORDERS`) | — | 5 | 5 |
| Statuts CANCELLED / DELIVERED / PLACED / RETURNED | — | 963 / 3 605 / 589 / 914 | 963 / 3 605 / 589 / 914 |
| Chiffre d'affaires livré | — | 838 711,64 | 838 711,64 |
| Commandes orphelines (client inconnu) | — | 5, signalées par un test dbt en `warn` | 5 |

Les valeurs attendues sont calculées par le générateur à partir des fichiers sources
(`scripts/generate_source_files.py`, `manifest.json`) et vérifiées à chaque exécution
par le test dbt de réconciliation `assert_marts_match_source_manifest`.

Pour rejouer la comparaison : [`snowflake/10_compare_migration.sql`](../snowflake/10_compare_migration.sql)
(une seule table de résultats, colonne `statut` = `OK` partout).

## Constats ADF et leur traitement dans la cible

| # | Constat ADF | Traitement dans Airflow |
|---|---|---|
| 1 | En-tête accepté seulement avec `\r\n` ; `SKIP_HEADER` supprimé sans avertissement | `COPY INTO` écrit dans le DAG ; le format `CSV_HEADER` lit l'en-tête (`PARSE_HEADER = TRUE`) au lieu de le sauter, quelles que soient les fins de ligne |
| 2 | Pas de traçabilité (`_source_file`, `_loaded_at` vides) | `INCLUDE_METADATA` : nom du fichier et heure de chargement sur chaque ligne |
| 3 | Pas d'idempotence (`FORCE = TRUE`) | `DELETE` des lignes du fichier puis `COPY INTO`, dans une seule transaction |
| 4 | Mappage positionnel | `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` (la colonne `channel`, apparue le 15/10, se charge sans changement) |
| 5 | Droits implicites (stage temporaire, `CREATE STAGE`, `CREATE FILE FORMAT`) | Droits explicites et minimaux dans `snowflake/05_roles.sql` (`DELETE` ajouté pour l'idempotence) |
| 6 | Reprise automatique utile | Conservée : `retries=2`, `retry_delay=1 min` |
| 7 | Dates du déclencheur en locale FR → erreur de publication | Planification dans le code, en UTC |

## Points d'attention rencontrés pendant la migration

1. **Authentification Snowflake.** Avec une clé privée, le connecteur récent exige
   `"authenticator": "SNOWFLAKE_JWT"` (erreur 251007 sinon).
2. **Date des données.** Avec `CronDataIntervalTimetable`, le jour chargé est le début de
   l'intervalle (`data_interval_start`), comme `windowStartTime` côté ADF.
   `airflow dags test <date>` prend l'intervalle qui se termine à `<date>` : un test manuel
   peut donc charger la veille. Le dry-run du backfill l'a montré avant le vrai rattrapage.
3. **Backfill.** Un DAG en pause n'exécute pas les tâches d'un backfill : il faut le réactiver.
4. **Cosmos et tests dbt.** Par défaut, un test qui lit plusieurs modèles est rattaché à chacun ;
   `TestIndirectSelection.CAUTIOUS` + `should_detach_multiple_parents_tests=True` lui donnent sa
   propre tâche. En test `pytest`, le cache `dbt ls` de Cosmos est désactivé (pas de base de
   métadonnées).
5. **Assets.** Un DAG planifié sur des Assets ne voit pas les événements antérieurs à sa création :
   premier lancement manuel de `dbt_retail` après le backfill.
6. **Sévérité `warn`.** Cosmos affiche un test dbt en `warn` comme une tâche verte : les 5 commandes
   orphelines ne sont visibles que dans les logs de la tâche.
7. **Fichiers générés par dbt.** `target/`, `logs/` et `.user.yml` sont exclus du dépôt.
