# 🛒 Olist E-Commerce Analysis

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Marimo](https://img.shields.io/badge/Marimo-Interactive-orange.svg)](https://marimo.io/)
[![DuckDB](https://img.shields.io/badge/DuckDB-SQL-yellow.svg)](https://duckdb.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Analyse descriptive et prédictive du dataset **Olist E-Commerce** (Brésil, 2016-2018).

## 📋 Description

Ce projet réalise une analyse approfondie des données de commandes de la plateforme e-commerce brésilienne **Olist** sur la période 2016-2018. L'objectif est de :

1. ✅ **Analyser** les données de manière descriptive avec visualisations interactives
2. ✅ **Nettoyer** et préparer les datasets pour exploitation
3. ✅ **Structurer** les données dans une base SQL avec schéma optimisé
4. 🔮 **Prédire** (optionnel) les tendances de commandes futures

## 🎯 Objectifs

### Obligatoires (Priorité 1)
- [x] Analyse descriptive approfondie des 9 fichiers CSV
- [x] Script Python de nettoyage de données (POO)
- [x] Notebooks Marimo interactifs pour visualisations
- [x] Base de données SQL avec schéma déduit
- [x] Documentation complète

### Optionnels (Priorité 2)
- [ ] Analyse prédictive des commandes
- [ ] Comparaison prédictions vs données réelles
- [ ] Dashboard interactif complet

## 📊 Dataset

- **Source** : [Kaggle - Olist Brazilian E-Commerce](https://www.kaggle.com/olistbr/brazilian-ecommerce)
- **Contenu** : 9 fichiers CSV avec commandes, clients, produits, paiements, reviews...
- **Période** : 2016-2018
- **Taille** : ~100k commandes

## 🚀 Démarrage Rapide

### Prérequis

- Python 3.8+
- `uv` (gestionnaire de paquets rapide)
- Compte Kaggle (pour téléchargement automatique)

### Installation

```powershell
# 1. Cloner le projet
git clone <URL>
cd olist-analysis

# 2. Créer environnement virtuel avec uv
uv venv
.venv\Scripts\activate  # PowerShell

# 3. Installer les dépendances
uv pip install -e .

# 4. Configuration Kaggle (optionnel, sinon téléchargement manuel)
# Placer kaggle.json dans ~/.kaggle/ ou C:\Users\<user>\.kaggle\
```

### Utilisation

```powershell
# Télécharger les données depuis Kaggle
uv run python scripts/download_data.py

# Valider la qualité des données avec Pandera
uv run python scripts/validate_data.py

# Nettoyer et préparer les données
uv run python scripts/process_data.py

# Charger dans la base de données DuckDB
uv run python scripts/load_to_db.py

# Lancer les notebooks interactifs Marimo
marimo edit notebooks/01_exploration.py
marimo edit notebooks/02_descriptive_analysis.py
```

## 📁 Structure du Projet

```
olist-analysis/
├── .github/
│   └── copilot-instructions.md    # Instructions pour GitHub Copilot
├── data/
│   ├── raw/                        # Données brutes Kaggle
│   ├── processed/                  # Données nettoyées
│   └── olist.duckdb               # Base de données
├── src/
│   ├── data/
│   │   ├── loader.py              # Chargement Kaggle
│   │   ├── cleaner.py             # Nettoyage (POO)
│   │   └── validator.py           # Validation qualité
│   ├── database/
│   │   ├── connection.py          # Connexion DB
│   │   ├── writer.py              # Écriture SQL
│   │   └── schema.py              # Schéma tables
│   ├── analysis/
│   │   ├── descriptive.py         # Stats descriptives
│   │   └── preprocessing.py       # Transformations
│   └── utils/
│       └── config.py              # Configuration
├── notebooks/
│   ├── 01_exploration.py          # Marimo: Exploration
│   ├── 02_descriptive_analysis.py # Marimo: Analyses
│   └── 03_data_quality.py         # Marimo: Qualité
├── scripts/
│   ├── download_data.py           # Script téléchargement
│   ├── process_data.py            # Script nettoyage
│   └── load_to_db.py              # Script chargement DB
├── tests/                          # Tests unitaires
├── .gitignore
├── pyproject.toml                  # Dépendances
└── README.md
```

## 📖 Documentation

### Modules Principaux

#### 1. Data Loading (`src/data/loader.py`)

```python
from src.data.loader import OlistDataLoader

loader = OlistDataLoader()
dataframes = loader.load_all()
print(loader.summary())
```

#### 2. Data Cleaning (`src/data/cleaner.py`)

```python
from src.data.cleaner import DataCleaner

cleaner = DataCleaner(df, name="orders")
cleaned_df = (
    cleaner
    .remove_duplicates()
    .handle_missing_values(strategy='drop')
    .convert_dtypes({'order_date': 'datetime64'})
    .get_cleaned_data()
)
cleaner.print_summary()
```

#### 3. Database Integration (`src/database/`)

```python
from src.database.connection import DatabaseConnection
from src.database.writer import DatabaseWriter

db_conn = DatabaseConnection(db_type='duckdb')
with db_conn:
    writer = DatabaseWriter(db_conn)
    writer.write_multiple(dataframes)
```

#### 4. Data Validation avec Pandera (`src/data/schemas.py`)

```python
from src.data.validator import DataValidator
from src.data.schemas import get_schema

# Validation avec schéma prédéfini
validator = DataValidator(orders_df, name="orders")
schema = get_schema('olist_orders_dataset')
is_valid = validator.validate_with_schema(schema)

# Ou utiliser le script de validation complet
# uv run python scripts/validate_data.py
```

## 🛠️ Technologies

| Outil          | Usage                              |
|----------------|------------------------------------|
| **Python**     | Langage principal                  |
| **uv**         | Gestion de dépendances rapide      |
| **Pandas**     | Manipulation de données            |
| **Pandera**    | Validation de schémas DataFrame    |
| **Marimo**     | Notebooks interactifs              |
| **DuckDB**     | Base de données SQL embarquée      |
| **kagglehub**  | Téléchargement dataset Kaggle      |
| **Plotly**     | Visualisations interactives        |
| **SQLAlchemy** | ORM pour schéma SQL                |

## 📈 Exemples d'Analyses

Les notebooks Marimo fournissent :

- 📊 Statistiques descriptives par table
- 📉 Évolution temporelle des commandes
- 🗺️ Distribution géographique des clients
- 💰 Analyse des prix et paiements
- ⭐ Analyse des reviews et satisfaction
- 🔗 Relations entre tables (clés étrangères)

## 🤝 Conventions de Commits

Format : `type(scope): description`

**Types** :
- `feat`: Nouvelle fonctionnalité
- `fix`: Correction de bug
- `docs`: Documentation
- `refactor`: Refactorisation
- `test`: Tests
- `chore`: Maintenance

**Exemples** :
```
feat(data): add Kaggle data loader
feat(analysis): add descriptive statistics
docs(readme): add installation instructions
refactor(cleaner): apply OOP pattern
```

## 🕒 Planning

| Phase | Durée | Tâches |
|-------|-------|--------|
| 1. Setup | 30 min | Installation, téléchargement données |
| 2. Exploration | 1h | Notebooks Marimo d'exploration |
| 3. Analyse | 2h | Analyses descriptives complètes |
| 4. Nettoyage | 1h30 | Classes POO de nettoyage |
| 5. SQL | 1h | Schéma et chargement en base |
| 6. Docs | 30 min | README, docstrings, commits |

**Temps total estimé** : ~6h30

## 📝 License

MIT License - Voir [LICENSE](LICENSE) pour plus de détails.

## 👤 Auteur

Projet réalisé dans le cadre de la formation Data Engineering Simplon.

---

**Dernière mise à jour** : 2026-02-09
