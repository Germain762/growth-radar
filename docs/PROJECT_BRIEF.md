# Growth Radar — Brief de projet

> Document à utiliser comme contexte initial pour toute conversation avec Claude
> sur ce projet. À coller en début de conversation ou à attacher comme fichier.

---

## 1. Profil et contexte

**Qui je suis** : data engineer avec un background **Microsoft/ETL classique**.
Outils maîtrisés au quotidien : SSIS, SSAS, Azure SQL Server, Azure Data Factory,
Microsoft Fabric, Power BI, Talend. Point fort : SQL, modélisation dimensionnelle
Kimball, ETL batch, écosystème Azure.

**Ce que je veux développer** : l'écosystème open-source moderne, la Python data
stack, l'orchestration code-first, les formats de fichiers modernes
(Parquet, Iceberg), les approches ELT vs ETL, les patterns lakehouse.

**Environnement de travail** : Windows 11, mais je travaillerai via **WSL2
(Ubuntu)** pour éviter les galères Spark/Docker sous Windows natif.

**Budget total projet** : ~50€, privilégier open-source et free-tiers. Pas
d'Azure (sauf pertinence pédagogique explicite).

---

## 2. Objectifs d'apprentissage prioritaires

1. Sortir de l'écosystème Microsoft pur et découvrir la **modern data stack**
2. Passer d'une logique ETL drag-and-drop (SSIS/ADF/Talend) à une logique
   **code-first** (Python, SQL versionné, IaC)
3. Comprendre les différences de paradigme : **ETL vs ELT**, warehouse vs
   lakehouse, scheduler vs orchestrator
4. Apprendre des outils à forte valeur marché hors-Microsoft : **dbt, Dagster,
   DuckDB, Spark, Parquet, MinIO**
5. Appliquer les bonnes pratiques d'ingénierie logicielle à la data : Git,
   tests, CI/CD, documentation, idempotence, schema evolution

**Positionnement CV** : projet **complémentaire** de mes compétences Microsoft,
pas redondant. Profil hybride "solide sur Microsoft ET à l'aise sur la modern
data stack open-source".

---

## 3. Vision du projet

### Concept métier
Construire un système qui identifie des **dynamiques sectorielles et
thématiques** sur les marchés actions, à trois niveaux de granularité, en
croisant signaux de marché, signaux narratifs et signaux d'émergence.

Le résultat final : un dashboard qui dit, à différents niveaux de zoom,
*"voici ce qui bouge cette semaine et pourquoi"*, avec la capacité de détecter
des thématiques émergentes que les classifications officielles ne connaissent
pas encore.

### Hiérarchie à trois niveaux
C'est le cœur conceptuel. Chaque entreprise appartient à :

- **Niveau 1 — GICS** (macro, stable, exhaustif) : 11 secteurs, 74 industries,
  163 sous-industries. Classification officielle, mono-appartenance. Exemple :
  Coherent → Information Technology > Technology Hardware & Equipment > Electronic Components
- **Niveau 2 — ETFs thématiques** (méso, curé, multi-appartenance) : ~30 ETFs
  sélectionnés avec leurs holdings pondérés. Une entreprise peut appartenir à
  plusieurs ETFs avec des poids différents. Exemple : Nvidia dans SOXX (~8%),
  BOTZ (~10%), SMH (~10%)
- **Niveau 3 — Clusters émergents** (micro, découvert, dynamique) : clusters
  détectés automatiquement par analyse de cooccurrences dans les news,
  regénérés hebdomadairement. **POC en Phase 1, industrialisé en Phase 2.**

### Flux analytique
Quand on observe un mouvement de prix sur une entreprise, on peut le
contextualiser aux trois niveaux : tout le secteur IT monte (niveau 1) ?
Spécifique à l'IA/robotique (niveau 2) ? Un cluster émergent particulier tire
le mouvement (niveau 3) ?

### Focus métier
Intérêt personnel pour la **photonique** (lasers, capteurs optiques, fibre,
silicon photonics, LiDAR, quantum photonics). Le projet sert aussi à alimenter
mes décisions d'investissement réelles. La photonique sert de "ground truth"
pour valider que le système détecte bien des clusters cohérents.

---

## 4. Scope : Option Hybride en 3 phases

### Phase 1 (6 semaines, focus CV + fondations)
- **Niveaux 1 et 2 complets et polis**
- **POC niveau 3** en notebook exploratoire (pas industrialisé)
- Infrastructure de qualité production-like
- Dashboard Streamlit sur les 2 niveaux
- **Livrable** : projet GitHub public, démonstrable, CI verte

### Phase 2 (après Phase 1)
- Industrialisation du niveau 3 : pipeline news + NLP + clustering automatisé
- Ingestion de signaux faibles enrichie (brevets, publications scientifiques)
- Éventuellement bascule Data Vault 2.0 pour apprentissage

### Phase 3 (après Phase 2)
- Couche streaming pour alertes temps réel (Redpanda/Kafka)
- Architecture Lakehouse avec Iceberg
- Éventuellement exploration Data Vault 2.0

---

## 5. Stack technique validée

### Infrastructure
- **OS dev** : WSL2 Ubuntu 22.04 sur Windows 11
- **Conteneurisation** : Docker Desktop avec backend WSL2, Docker Compose
- **Stockage objet** : MinIO (S3-compatible, local)
- **Format de fichiers** : Parquet partitionné par date/mois
- **Versioning** : Git + GitHub

### Python stack
- **Python 3.11-3.12** (compatibilité Spark)
- **Gestionnaire de paquets** : uv (moderne, écrit en Rust)
- **Validation de schémas** : Pydantic
- **HTTP client** : httpx + tenacity (retry avec backoff)
- **Logs** : structlog (logs structurés)

### Ingestion
- **yfinance** : prix OHLCV, infos ticker (gratuit, sans clé)
- **FRED API** : données macro (gratuit, clé requise)
- **Holdings ETFs** : scraping custom iShares/Global X + backfill historique ARK
- **News** : GDELT (Phase 2) ou NewsAPI (POC Phase 1)
- **SEC EDGAR** : filings XBRL (Phase 2+)

### Stockage et traitement
- **Bronze** (raw) : Parquet sur MinIO, partitionné par date, immutable
- **Silver** (enrichi) : Parquet sur MinIO, produit par Spark ou DuckDB selon
  le cas d'usage
- **Gold** (marts) : produit par dbt

### Choix clé : DuckDB vs Spark
- **DuckDB + dbt** : prix, holdings récents, macro, toute transformation
  tabulaire classique (500-2M lignes)
- **Spark (PySpark)** : **uniquement** pour les tâches lourdes et
  parallélisables qui le justifient :
  - Traitement de l'historique complet des holdings ETFs (window functions
    sur ~2M lignes, calculs de contribution par holding)
  - Phase 2+ : NLP sur news (entity extraction, embeddings, cooccurrences)
- **Pas de Spark sur les prix** (DuckDB fait mieux et plus vite)

### Orchestration
- **Dagster** (plutôt qu'Airflow) : approche asset-oriented plus pédagogique
  pour quelqu'un qui pense en modélisation dimensionnelle
- Assets, schedules, sensors, backfills, retry, lineage

### Modélisation
- **dbt-core + dbt-duckdb** : SQL versionné, modulaire, testé, documenté
- Lineage automatique via `dbt docs`
- Tests : unique, not_null, relationships, dbt_utils.accepted_range

### Serving
- **Streamlit** : dashboard principal avec drilldown entreprise
- **Metabase** (optionnel) : exploration ad hoc

### Qualité et CI
- **ruff** : lint et formatage Python
- **pytest** : tests unitaires Python
- **dbt tests** : qualité des modèles
- **pre-commit** : hooks Git
- **GitHub Actions** : CI (tests + dbt compile à chaque PR)

---

## 6. Architecture cible

```
Sources          → Bronze          → Silver              → Gold
(APIs, scrapers)   (Parquet raw)    (Parquet enrichi)     (dbt marts)
                                    Spark ou DuckDB        DuckDB

                  Orchestration : Dagster (assets, schedules, sensors)

                  Serving : Streamlit (dashboard 2 niveaux)
                            Metabase (ad hoc, optionnel)

                  Cross-cutting : Git + CI GitHub Actions,
                                  Pydantic, dbt tests, structlog,
                                  Docker Compose, dotenv pour secrets
```

### Stratégie Spark
- **Phase 1** : PySpark **local standalone** dans le venv (pip install pyspark)
- **Fin Phase 1 ou Phase 2** : migration vers **cluster Docker Compose**
  (master + 2 workers) pour apprendre le vrai comportement distribué

---

## 7. Modèle dimensionnel final (Phase 1)

### Approche : Kimball en étoile/constellation
Data Vault 2.0 envisagé seulement en Phase 3 comme apprentissage
complémentaire.

### Dimensions (2 seulement)

**`dim_instrument`** (unifiée, couvre equities + ETFs + indices)
- `instrument_sk` (PK, hash de ticker)
- `ticker_nk`, `instrument_type` ('equity' | 'etf' | 'index' | 'commodity')
- `name`, `exchange`, `currency`
- `gics_sector_name`, `gics_industry_group_name`, `gics_industry_name`,
  `gics_sub_industry_name` (dénormalisés, NULL pour ETFs)
- `country_hq` (NULL pour ETFs)
- `etf_issuer`, `etf_type`, `thematic_tags` (array, NULL pour equities)
- `is_active`, `valid_from`, `valid_to`, `is_current` (SCD2)
- **Implémentation SCD2** : d'abord manuel (spike d'apprentissage ~1 soirée),
  puis migration vers `dbt snapshots` pour la version finale

**`dim_date`** (ephemeral, générée à la volée)
- `date_sk`, `date_day`, `year`, `quarter`, `month`, `week`, `day_of_week`
- `is_weekend`, `is_trading_day_approx`
- Role-playing via alias dans les faits

### Faits (2 en Phase 1)

**`fct_price_daily`**
- Grain : instrument × date
- `price_sk` (PK, hash), `instrument_sk`, `price_date_sk`
- OHLCV + `adj_close` + `volume`
- Returns précalculés : `return_1d`, `return_5d`, `return_21d`, `return_63d`,
  `return_252d`
- `volatility_21d`, `volume_ratio_21d`
- Matérialisation : `incremental`, partitionnée par mois

**`fct_company_etf_membership`** (bridge historisé)
- Grain : company × etf × composition_date
- Clé composite hash
- `company_sk`, `etf_sk`, `composition_date_sk`
- `weight_pct`, `shares_held`, `market_value_usd`
- `holding_rank` (window function sur weight_pct)
- Produit par le job Spark historique
- Matérialisation : `incremental`, partitionnée par mois

### Marts de consommation

- **`mart_momentum_gics`** : performance par secteur GICS vs S&P 500, plusieurs
  fenêtres (5j, 21j, 63j, 252j)
- **`mart_momentum_etf`** : performance par ETF thématique, contribution des
  holdings principaux
- **`mart_company_profile`** : vue 360 par entreprise (prix, returns, vol,
  ETFs d'appartenance, position sectorielle)

### Conventions
- Préfixes : `stg_`, `int_`, `dim_`, `fct_`, `mart_`
- Surrogate keys : `*_sk` via `dbt_utils.generate_surrogate_key` (hash
  déterministe, pas d'IDENTITY)
- Natural keys : `*_nk`
- Matérialisations : view (staging), ephemeral (intermediate), table (dim),
  incremental (fact), table (mart)
- Tests obligatoires : unique + not_null sur PK, relationships sur FK

### Simplifications appliquées (vs modèle initial plus verbeux)
- **Fusion `dim_company` + `dim_etf` → `dim_instrument`** (moins de jointures,
  requêtes unifiées)
- **Pas de `dim_gics` séparée** : GICS dénormalisé dans `dim_instrument`
- **Pas de `fct_macro_daily`** en Phase 1 : données FRED en staging seulement
- **`dim_date` ephemeral** plutôt que matérialisée

---

## 8. Plan de développement Phase 1 (6 semaines, ~6-8h/semaine)

| Semaine | Focus | Livrable démontrable |
|---|---|---|
| **S1** | Fondations : WSL2, Docker, MinIO, repo, uv, Dagster, première ingestion yfinance sur 50 tickers | Bucket `bronze/` avec partitions date |
| **S2** | Niveau 1 GICS : ingestion 500 tickers + ETFs sectoriels XLK/XLF/etc., dbt init, `dim_instrument`, staging, intermediate | `mart_momentum_gics` avec tests dbt |
| **S3** | ETFs + Spark : ingestion holdings 10-15 ETFs thématiques, backfill ARK, job Spark `fct_company_etf_membership` avec window functions | Table `fct_company_etf_membership` peuplée |
| **S4** | Niveau 2 : `mart_momentum_etf`, contribution par holding, croisement GICS × ETFs | Requêtes métier fonctionnelles sur 2 niveaux |
| **S5** | POC niveau 3 : notebook GDELT/NewsAPI 1 mois, spaCy entities, cooccurrences, Leiden via `leidenalg`, top-terms | Notebook documenté dans `experiments/` |
| **S6** | Dashboard + polish : Streamlit 2 niveaux, drilldown, CI GitHub Actions, ADRs, README portfolio-grade, dockerisation | Projet GitHub public démo-able |

---

## 9. Liste des ETFs cibles (à valider/affiner en S2-S3)

### Sectoriels GICS (benchmarks niveau 1)
- XLK (Tech), XLF (Finance), XLE (Energy), XLV (Healthcare), XLY (Conso disc.),
  XLP (Conso staples), XLI (Industrial), XLB (Materials), XLU (Utilities),
  XLRE (Real Estate), XLC (Comm. services)

### Thématiques (niveau 2)
- **Semi-conducteurs** : SOXX, SMH
- **IA/Robotique** : BOTZ, ROBO, IRBO
- **Cybersécurité** : CIBR, HACK
- **Clean energy** : ICLN, QCLN
- **Space/Defense** : ITA, UFO
- **Innovation** : ARKK, ARKG, ARKQ (bonus : historique quotidien disponible)
- **China tech** : KWEB, CQQQ
- **Fintech** : FINX, ARKF

Focus indirect photonique via SOXX/SMH (semi-conducteurs) et ITA (défense/LiDAR).

---

## 10. Ce que j'attends de Claude

1. **Challenger mes réflexes Microsoft** : signaler quand je raisonne en mode
   SSIS/ADF et montrer l'équivalent moderne avec le gain associé
2. **Proposer des alternatives** : pour chaque brique, 2-3 options open-source
   avec forces/faiblesses
3. **Expliquer les trade-offs** : coût (budget 50€ !), courbe d'apprentissage,
   maturité, vendor lock-in, complexité opérationnelle
4. **Privilégier l'apprentissage aux solutions magiques** : si un outil cache
   trop de concepts, proposer d'abord une version plus manuelle
5. **Penser production-ready** : tests, idempotence, gestion d'erreurs,
   monitoring, documentation, schema evolution, reprise sur incident
6. **Donner du concret** : snippets Python, modèles dbt, DAGs, structure de
   repo, Dockerfiles, commandes CLI
7. **Proposer des bifurcations pédagogiques** sans les imposer
8. **Format structuré mais concis** : signaler les "gotchas" classiques, code
   commenté, liens doc officielle si pertinent

---

## 11. Points de vigilance identifiés

- **Schema evolution des APIs** : yfinance, SEC EDGAR, holdings ETFs changent
  régulièrement leurs formats. Valider avec Pydantic dès l'ingestion.
- **Rate limits** : SEC EDGAR exige un User-Agent identifiant (email) et limite
  à 10 req/sec. yfinance n'a pas de rate limit officiel mais se fait bloquer si
  trop agressif.
- **Idempotence** : partitionnement par date + `dbt incremental` pour tout
  rejouer sans effets de bord.
- **Timezones** : yfinance en UTC, SEC en EST, Europe en CET. Normaliser en
  UTC dès le staging.
- **Tickers changeants** : META (ex-FB), X (ex-TWTR). Stocker `former_tickers`
  en array avec fallback en staging.
- **Splits et dividendes** : TOUJOURS utiliser `adj_close` pour les returns,
  jamais `close`.
- **Historique ETFs** : ARK fournit l'historique complet, les autres
  uniquement la composition courante. Accepter de "démarrer à zéro" sur ces
  ETFs et capturer quotidiennement.
- **GDELT volumineux** : filtrer à l'ingestion (langue, thèmes, fenêtre
  temporelle) pour éviter de saturer le disque.
- **Piège Spark débutant** : ne JAMAIS faire `.toPandas()` ou `.collect()` sur
  un gros DataFrame. Écrire directement en Parquet.
- **Java pour Spark** : Java 11 ou 17 exigé (pas 21+). Via
  `sudo apt install openjdk-17-jdk` dans WSL.

---

## 12. État actuel du projet

**À date** : conception validée, setup pas encore démarré.

**Prochaine étape** : Semaine 1 — setup environnement complet (WSL2, Docker,
VS Code Remote-WSL, repo GitHub, venv uv, Docker Compose MinIO, première
ingestion yfinance).
