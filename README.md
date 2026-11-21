# Migration Azure Data Factory → Airflow 3 (Astronomer) · dbt · Snowflake

[![CI](https://github.com/omarboukherys/adf-to-astronomer-migration/actions/workflows/ci.yml/badge.svg)](https://github.com/omarboukherys/adf-to-astronomer-migration/actions/workflows/ci.yml)

Migration de bout en bout d'un chargement quotidien **Azure Blob → Snowflake** orchestré par
**Azure Data Factory** vers **Apache Airflow 3 sur Astronomer**, avec des transformations
**dbt** exécutées par **Astronomer Cosmos**.

Le pipeline ADF « legacy » a d'abord été construit et exécuté pour de vrai, puis migré.
Les deux orchestrateurs ont chargé les mêmes 90 fichiers sources. La comparaison dans
Snowflake donne **11 contrôles sur 11 conformes**.

## Résultats

| | ADF (legacy) | Airflow 3 (cible) |
|---|---|---|
| Lignes brutes customers / products / orders | 2 001 / 96 / 11 630 | 2 001 / 96 / 11 630 |
| Lignes tracées jusqu'à leur fichier source | 0 % | 100 % |
| Relance d'un jour déjà chargé | doublons | aucun doublon (idempotent) |
| Correspondance des colonnes | par position | par nom |
| Secret Azure dans l'orchestrateur | identité managée de la fabrique | aucun |
| Transformations | aucune | dbt : 8 modèles, tests, réconciliation |
| Marts : clients · produits · commandes valides · rejetées | — | 1 831 · 80 · 6 071 · 5 |
| Chiffre d'affaires livré | — | 838 711,64 (au centime près) |

Le détail objet par objet est dans [`docs/migration-mapping.md`](docs/migration-mapping.md),
et les constats faits sur ADF sont dans [`docs/adf-legacy.md`](docs/adf-legacy.md).

## Architecture

```
Azure Blob (conteneur landing, 3 CSV par jour)
        │  storage integration Snowflake (Airflow ne détient aucune clé Azure)
        ▼
Stage externe @RETAIL.LANDING.AZURE_LANDING
        │
        ├── ADF (legacy) : déclencheur fenêtre bascule → ForEach → copie directe → RAW_ADF
        │
        └── Airflow 3 / Astronomer
              load_landing_to_snowflake (1 exécution par jour, backfill)
                └── par entité : wait_for_file (SqlSensor) → load_file (DELETE + COPY INTO, 1 transaction)
                      └── publie l'Asset retail.raw_airflow.<entité>
                                │  (planification par les données, pas par l'heure)
                                ▼
              dbt_retail (Cosmos : 1 tâche par modèle dbt + ses tests)
                staging (vues) → marts (tables) → test de réconciliation
```

## Points clés de la cible

- **Idempotence** : chaque fichier est rechargé par `DELETE` de ses lignes puis `COPY INTO`,
  dans une seule transaction. Rejouer un jour remplace ses lignes au lieu de les dupliquer.
- **Traçabilité** : `_source_file` et `_loaded_at` sont renseignés sur chaque ligne (`INCLUDE_METADATA`).
- **Évolution de schéma** : chargement par nom de colonne. La colonne `channel`, apparue
  le 15/10, se charge sans modification.
- **Aucun secret Azure dans Airflow** : Snowflake lit le Blob via une storage integration.
  Les comptes de service Snowflake s'authentifient par paire de clés, et les rôles sont au
  moindre privilège ([`snowflake/05_roles.sql`](snowflake/05_roles.sql)).
- **Data-aware scheduling** : le DAG dbt démarre dès que les 3 tables brutes ont été
  rafraîchies (Assets Airflow 3). Il n'y a pas d'horaire à synchroniser.
- **Qualité** : tests dbt (unicité, non-nullité, valeurs acceptées, intégrité référentielle)
  et test de réconciliation des marts contre les attendus calculés depuis les fichiers sources.
  Les 5 commandes orphelines sont signalées en `warn`, sans bloquer la chaîne.
- **Coûts** : warehouse `XSMALL` dédié (`ETL_WH`), arrêt automatique après 60 s.
- **CI** : chaque push lance `astro dev pytest` (25 tests) dans l'image Astro Runtime.

## Stack

| Composant | Version |
|---|---|
| Astro Runtime (Airflow 3) | 3.3-12 (Python 3.14) |
| Astro CLI | 1.47.0 |
| astronomer-cosmos | 1.16.0 |
| dbt-core / dbt-snowflake (venv dédié) | 1.12.7 / 1.12.5 |
| apache-airflow-providers-snowflake | 6.17.0 |
| Snowflake | Enterprise, AWS eu-west-3 (Paris) |
| Azure | Blob Storage, Data Factory |

## Arborescence

```
adf/arm_template/      export ARM du pipeline ADF legacy
dags/
  load_landing_to_snowflake.py   chargement Blob → RAW_AIRFLOW (remplace le pipeline ADF)
  dbt_retail.py                  transformations dbt via Cosmos
docs/
  adf-legacy.md                  le pipeline ADF et ses 7 constats
  migration-mapping.md           correspondance ADF → Airflow et preuve chiffrée
include/dbt/retail/    projet dbt (staging, marts, tests, réconciliation)
scripts/generate_source_files.py générateur des fichiers sources (données fictives)
snowflake/             scripts 00 → 10 (base, stage, tables, rôles, comptes, contrôles)
tests/                 tests pytest des DAGs, du rendu Cosmos et du générateur
.github/workflows/ci.yml         CI GitHub Actions
```

## Lancer le projet

Prérequis : Docker Desktop, [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli),
un compte Snowflake et un compte de stockage Azure.

1. **Données sources** (fictives, déterministes) :
   ```
   python scripts/generate_source_files.py --output data --seed 2026
   ```
   Elles sont générées dans `data/landing/` (non versionné), avec `manifest.json` qui contient
   les attendus. Téléverser ensuite les CSV dans le conteneur `landing`, par exemple :
   ```
   az storage blob upload-batch --account-name <compte> --destination landing --source data/landing --pattern "*.csv" --auth-mode login
   ```
2. **Snowflake** : exécuter `snowflake/00_setup.sql` à `06_users.sql` dans l'ordre. Les clés
   des comptes de service sont générées hors du dépôt (voir `06_users.sql`), et la clé privée
   de `SVC_AIRFLOW` est copiée dans `include/keys/` (ignoré par git et par Docker).
3. **Connexion Airflow** : créer un fichier `.env` à la racine (non versionné) :
   ```
   AIRFLOW_CONN_SNOWFLAKE_DEFAULT={"conn_type":"snowflake","login":"SVC_AIRFLOW","schema":"RAW_AIRFLOW","extra":{"account":"<compte>","warehouse":"ETL_WH","database":"RETAIL","role":"AIRFLOW_ROLE","authenticator":"SNOWFLAKE_JWT","private_key_file":"/usr/local/airflow/include/keys/svc_airflow_key.p8"}}
   ```
4. **Démarrer Airflow** :
   ```
   astro dev start --wait 5m
   ```
5. **Charger l'historique** : activer `load_landing_to_snowflake`, puis lancer un backfill
   du 01/10 au 30/10/2026 (dans l'interface : bouton Trigger, puis Backfill ; ou en CLI, avec `--dry-run` d'abord) :
   ```
   astro dev run backfill create --dag-id load_landing_to_snowflake --from-date 2026-10-01 --to-date 2026-10-30 --dry-run
   ```
   `dbt_retail` se déclenche ensuite via les Assets. S'il a été créé après les chargements,
   un premier lancement manuel est nécessaire.
6. **Contrôler** : `snowflake/10_compare_migration.sql` (colonne `statut` = `OK` partout).

Tests (sans Snowflake) :
```
astro dev pytest tests
```

## Pistes suivantes

- Déploiement sur un Deployment Astro (`astro deploy`) avec la connexion dans un gestionnaire de secrets.
- Alertes en cas d'échec (callbacks Airflow) et `dbt source freshness` sur les tables brutes.
- Lint du code (ruff) dans la CI.
