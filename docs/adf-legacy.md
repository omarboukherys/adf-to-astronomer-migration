# Le pipeline ADF « legacy »

Pour que la migration parte d'un vrai existant, le chargement quotidien a d'abord été
construit dans Azure Data Factory, tel qu'on le trouve souvent chez les clients.
Son export ARM est dans [`adf/arm_template/`](../adf/arm_template/).

## Ce qu'il fait

```
tr_daily_tumbling (fenêtre bascule, 24 h, 01/10 → 31/10/2026, 5 fenêtres en parallèle)
  └── pl_load_daily_landing (load_date = début de la fenêtre)
        └── ForEach_entity (customers, products, orders en parallèle)
              └── Copy_blob_to_snowflake (copie directe, 2 nouvelles tentatives à 60 s)
                    ds_landing_csv : landing/<entity>/<entity>_<load_date>.csv
                    → ds_snowflake_raw : RETAIL.RAW_ADF.<ENTITY>
```

| Objet | Rôle |
|---|---|
| `ls_blob_landing` | Azure Blob, authentification par identité managée de la fabrique |
| `ls_snowflake` | Snowflake V2, paire de clés du compte de service `SVC_ADF` (rôle `ADF_LOADER`) |
| `ds_landing_csv` | CSV paramétré (`entity`, `load_date`), en-tête, fins de ligne `\r\n` |
| `ds_snowflake_raw` | Table paramétrée du schéma `RAW_ADF` |
| `pl_load_daily_landing` | Boucle sur les entités et copie directe via la storage integration `AZURE_LANDING_INT` |
| `tr_daily_tumbling` | Une exécution par jour, rattrapage des jours passés |

Résultat : 30 exécutions réussies (21 à 27 s chacune), 90 copies,
2 001 / 96 / 11 630 lignes brutes chargées, conformes aux fichiers sources.

## Constats (à traiter pendant la migration)

1. **En-tête et fins de ligne.** En copie directe vers Snowflake, ADF n'accepte la ligne
   d'en-tête qu'avec des fins de ligne `\r\n`. Un `SKIP_HEADER` passé en option de format
   supplémentaire est **supprimé sans avertissement** du stage qu'ADF génère : l'en-tête a d'abord
   été chargé comme une ligne de données. Correction : fichiers en CRLF (RFC 4180).
2. **Pas de traçabilité.** La copie directe interdit colonnes additionnelles et mappage :
   `_source_file` et `_loaded_at` restent vides. Impossible de savoir de quel fichier vient une ligne.
3. **Pas d'idempotence.** Le `COPY INTO` généré contient `FORCE = TRUE` : chaque relance d'un jour
   recharge ses fichiers et **duplique** les lignes. Un rattrapage exige de vider les tables avant.
4. **Mappage positionnel.** ADF génère `SELECT $1, $2, … FROM @stage` : le bon chargement dépend de
   l'ordre des colonnes de la table, pas de leur nom.
5. **Droits implicites.** ADF crée un stage temporaire `adf_stage_import_<guid>` dans le schéma
   cible : le rôle de chargement a besoin de `CREATE STAGE` (et `CREATE FILE FORMAT`) sur ce schéma,
   en plus de `USAGE` sur la storage integration.
6. **Reprise automatique utile.** Avec 2 nouvelles tentatives à 60 s, l'exécution a réussi d'elle-même
   une fois le droit manquant ajouté (même ID d'exécution).
7. **Interface.** Les dates du déclencheur saisies dans une interface en français ont provoqué une
   erreur de publication (`RangeError: Invalid time value`) ; les dates ont été fixées en ISO 8601
   dans le JSON du déclencheur.

Ces points sont repris dans la cible Airflow : chargement par nom de colonne, colonnes de
traçabilité, chargements idempotents et rejouables jour par jour.
