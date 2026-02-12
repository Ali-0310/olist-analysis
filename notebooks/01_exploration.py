"""Notebook Marimo - Exploration initiale du dataset Olist."""

import marimo

__generated_with = "0.8.22"
app = marimo.App(layout_file="layouts/01_exploration.slides.json")


@app.cell
def __(mo):
    mo.md("""# 🛒 Exploration du Dataset Olist""")
    return


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import plotly.express as px
    import sys
    from pathlib import Path

    # Ajouter src au path
    sys.path.insert(0, str(Path.cwd().parent))

    from src.data.loader import OlistDataLoader
    return OlistDataLoader, Path, mo, pd, px, sys


@app.cell
def __(mo):
    mo.md("""## 1. Chargement des données""")
    return


@app.cell
def __(OlistDataLoader):
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    return dataframes, loader


@app.cell
def __(dataframes, mo):
    mo.md(f"**{len(dataframes)} fichiers chargés** depuis Kaggle")
    return


@app.cell
def __(mo):
    mo.md("""## 2. Résumé des fichiers""")
    return


@app.cell
def __(loader, mo):
    summary = loader.summary()
    mo.ui.table(summary)
    return (summary,)


@app.cell
def __(mo):
    mo.md("""## 3. Sélectionner un fichier à explorer""")
    return


@app.cell
def __(dataframes, mo):
    file_selector = mo.ui.dropdown(
        options=list(dataframes.keys()),
        value=list(dataframes.keys())[0] if dataframes else None,
        label="Choisir un fichier:"
    )
    file_selector
    return (file_selector,)


@app.cell
def __(dataframes, file_selector):
    if file_selector.value:
        selected_df = dataframes[file_selector.value]
    return (selected_df,)


@app.cell
def __(file_selector, mo, pd, selected_df):
    # Onglet 1: Aperçu
    apercu_content = mo.vstack([
        mo.md(f"""
        ### 📊 Aperçu: {file_selector.value}

        - **Lignes**: {len(selected_df):,}
        - **Colonnes**: {len(selected_df.columns)}
        - **Mémoire**: {selected_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB
        """),
        mo.md("#### Données complètes"),
        mo.ui.table(selected_df)
    ])

    # Onglet 2: Premières lignes
    premieres_lignes_content = mo.vstack([
        mo.md("### 📋 Premières lignes (Top 10)"),
        mo.ui.table(selected_df.head(10))
    ])

    # Onglet 3: Informations colonnes
    info_data = []
    for col in selected_df.columns:
        info_data.append({
            'Colonne': col,
            'Type': str(selected_df[col].dtype),
            'Non-null': selected_df[col].count(),
            'Null': selected_df[col].isnull().sum(),
            'Null %': f"{selected_df[col].isnull().sum() / len(selected_df) * 100:.2f}%",
            'Unique': selected_df[col].nunique()
        })

    info_df = pd.DataFrame(info_data)

    info_colonnes_content = mo.vstack([
        mo.md("### 📊 Informations sur les colonnes"),
        mo.ui.table(info_df)
    ])

    # Créer les onglets
    tabs = mo.ui.tabs({
        "📊 Aperçu": apercu_content,
        "📋 Premières lignes": premieres_lignes_content,
        "ℹ️ Informations colonnes": info_colonnes_content
    })

    tabs
    return (
        apercu_content,
        col,
        info_colonnes_content,
        info_data,
        info_df,
        premieres_lignes_content,
        tabs,
    )


@app.cell
def __(mo):
    mo.md("""## 4. Chargement dans la base de données (Bronze Layer)""")
    return


@app.cell
def __(mo):
    mo.md(
        """
        Cette section permet de charger les données brutes dans le schéma **Bronze** de la base de données SQLite.

        L'architecture Medallion organise les données en 3 couches :</br>
        - 🥉 **Bronze** : Données brutes depuis Kaggle</br>
        - 🥈 **Silver** : Données nettoyées et validées</br>
        - 🥇 **Gold** : Métriques et agrégations business</br>
        """
    )
    return


@app.cell
def __():
    from src.database.connection import DatabaseConnection
    from src.database.writer import DatabaseWriter
    from src.utils.config import config
    return DatabaseConnection, DatabaseWriter, config


@app.cell
def __(mo):
    load_to_bronze_button = mo.ui.run_button(
        label="🥉 Charger dans Bronze",
        kind="success"
    )
    load_to_bronze_button
    return (load_to_bronze_button,)


@app.cell
async def __(
    DatabaseConnection,
    DatabaseWriter,
    config,
    dataframes,
    load_to_bronze_button,
    mo,
):
    if load_to_bronze_button.value:
        # Connexion à SQLite
        db_conn = DatabaseConnection(
            db_type=config.DB_TYPE,
            db_path=config.DB_PATH,
            create_schemas=True
        )

        with db_conn:
            writer = DatabaseWriter(db_conn)

            # Écriture dans Bronze
            writer.write_multiple(dataframes, schema='bronze', if_exists='replace')

        bronze_result = mo.md(f"""
        ### ✅ Chargement terminé !

        - **Base de données**: `{config.DB_PATH}`
        - **Tables créées**: {len(dataframes)} tables dans le schéma Bronze
        - **Total lignes**: {sum(len(df) for df in dataframes.values()):,}

        **Prochaines étapes** :
        1. Nettoyer les données → Silver layer (`process_data.py`)
        2. Créer les métriques → Gold layer (`create_gold_metrics.py`)
        """)
    else:
        bronze_result = mo.md("_Cliquez sur le bouton ci-dessus pour charger les données dans Bronze_")

    bronze_result
    return bronze_result, db_conn, writer


if __name__ == "__main__":
    app.run()
