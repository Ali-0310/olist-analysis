"""Notebook Marimo - Analyses descriptives détaillées."""

import marimo

__generated_with = "0.8.22"
app = marimo.App(layout_file="layouts/02_descriptive_analysis.slides.json")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path.cwd().parent))

    from src.database.connection import DatabaseConnection
    from src.database.writer import DatabaseWriter
    from src.analysis.descriptive import DescriptiveAnalysis
    from src.utils.config import config
    return (
        DatabaseConnection,
        DatabaseWriter,
        DescriptiveAnalysis,
        Path,
        config,
        go,
        mo,
        pd,
        px,
        sys,
    )


@app.cell
def __(mo):
    mo.md(
        """
        # 📊 Analyses Descriptives Approfondies - Olist Dataset

        Ce notebook présente une analyse descriptive complète des données Olist chargées dans la couche **Bronze**.
        """
    )
    return


@app.cell
def __(mo):
    mo.md("""## 1️⃣ Chargement des données depuis Bronze Layer""")
    return


@app.cell
async def __(DatabaseConnection, config, mo, pd):
    # Connexion à SQLite Bronze
    db_conn = DatabaseConnection(
        db_type=config.DB_TYPE,
        db_path=config.DB_PATH
    )

    # Charger toutes les tables Bronze
    bronze_tables = {}
    with db_conn:
        conn = db_conn.connection
        cursor = conn.cursor()

        # Lister les tables Bronze
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'bronze_%'")
        table_names = [row[0] for row in cursor.fetchall()]

        # Charger chaque table
        for _table_name in table_names:
            clean_name = _table_name.replace('bronze_', '')
            bronze_tables[clean_name] = pd.read_sql_query(f"SELECT * FROM {_table_name}", conn)

    mo.md(f"""
    ✅ **{len(bronze_tables)} tables Bronze chargées**

    Tables disponibles : {', '.join(bronze_tables.keys())}
    """)
    return bronze_tables, clean_name, conn, cursor, db_conn, table_names


@app.cell
def __(mo):
    mo.md(
        """
        ## 2️⃣ Analyse Descriptive par Table

        Sélectionnez une table pour voir son analyse détaillée.
        """
    )
    return


@app.cell
def __(bronze_tables, mo):
    table_selector = mo.ui.dropdown(
        options=list(bronze_tables.keys()),
        value=list(bronze_tables.keys())[0] if bronze_tables else None,
        label="📁 Sélectionner une table:"
    )
    table_selector
    return (table_selector,)


@app.cell
def __(DescriptiveAnalysis, bronze_tables, table_selector):
    if table_selector.value:
        current_df = bronze_tables[table_selector.value]
        analyzer = DescriptiveAnalysis(current_df, name=table_selector.value)
    return analyzer, current_df


@app.cell
def __(current_df, mo, table_selector):
    mo.md(f"""
    ### 📋 Vue d'ensemble: {table_selector.value}

    - **Lignes**: {len(current_df):,}
    - **Colonnes**: {len(current_df.columns)}
    - **Mémoire**: {current_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB
    """)
    return


@app.cell
def __(analyzer, mo):
    # Onglets pour les différentes analyses

    # Onglet 1: Informations colonnes
    col_info = analyzer.column_info()
    tab1_content = mo.vstack([
        mo.md("### 📊 Informations détaillées par colonne"),
        mo.md("_Type de données, valeurs null, valeurs uniques_"),
        mo.ui.table(col_info)
    ])

    # Onglet 2: Statistiques descriptives
    stats = analyzer.basic_stats()
    if not stats.empty:
        tab2_content = mo.vstack([
            mo.md("### 📈 Statistiques descriptives (colonnes numériques)"),
            mo.ui.table(stats)
        ])
    else:
        tab2_content = mo.md("_Aucune colonne numérique dans cette table_")

    # Onglet 3: Top valeurs pour chaque colonne
    top_values_sections = []
    for col in analyzer.df.columns:
        if analyzer.df[col].nunique() <= 100:  # Seulement pour colonnes avec peu de valeurs uniques
            top_vals = analyzer.get_top_values(col, top_n=3)
            top_values_sections.append(
                mo.md(f"""
                **{col}** (Top 3):
                """)
            )
            top_values_sections.append(mo.ui.table(top_vals))

    if top_values_sections:
        tab3_content = mo.vstack([
            mo.md("### 🏆 Top 3 des valeurs les plus fréquentes"),
            mo.md("_Pour les colonnes catégorielles (≤ 100 valeurs uniques)_"),
            *top_values_sections
        ])
    else:
        tab3_content = mo.md("_Aucune colonne catégorielle avec peu de valeurs uniques_")

    analysis_tabs = mo.ui.tabs({
        "📊 Colonnes": tab1_content,
        "📈 Statistiques": tab2_content,
        "🏆 Top Valeurs": tab3_content
    })

    analysis_tabs
    return (
        analysis_tabs,
        col,
        col_info,
        stats,
        tab1_content,
        tab2_content,
        tab3_content,
        top_vals,
        top_values_sections,
    )


@app.cell
def __(mo):
    mo.md(
        """
        ### 📊 Visualisations - Distribution des Valeurs

        Sélectionnez une colonne pour voir la distribution de ses valeurs.
        """
    )
    return


@app.cell
def __(analyzer, mo):
    column_selector = mo.ui.dropdown(
        options=list(analyzer.df.columns),
        value=list(analyzer.df.columns)[0] if len(analyzer.df.columns) > 0 else None,
        label="Choisir une colonne:"
    )
    column_selector
    return (column_selector,)


@app.cell
def __(analyzer, column_selector, current_df, mo, px):
    if column_selector.value:
        selected_col = column_selector.value

        # Distribution des valeurs
        if current_df[selected_col].nunique() <= 50:
            # Pour colonnes catégorielles: bar chart
            dist = analyzer.value_distribution(selected_col, max_categories=50)

            fig = px.bar(
                dist,
                x='Valeur',
                y='Count',
                title=f"Distribution de {selected_col}",
                labels={'Valeur': selected_col, 'Count': 'Fréquence'},
                text='Count'
            )
            fig.update_traces(textposition='outside')

            viz_content = mo.vstack([
                mo.md(f"""
                **Distribution de la colonne: {selected_col}**
                - Valeurs uniques: {current_df[selected_col].nunique()}
                - Valeurs null: {current_df[selected_col].isnull().sum()} ({current_df[selected_col].isnull().sum()/len(current_df)*100:.2f}%)
                """),
                mo.ui.plotly(fig),
                mo.ui.table(dist.head(20))
            ])
        else:
            # Pour colonnes numériques ou trop de catégories: histogram
            fig = px.histogram(
                current_df,
                x=selected_col,
                title=f"Histogramme de {selected_col}",
                labels={selected_col: selected_col},
                nbins=50
            )

            viz_content = mo.vstack([
                mo.md(f"""
                **Histogramme de la colonne: {selected_col}**
                - Valeurs uniques: {current_df[selected_col].nunique()}
                - Valeurs null: {current_df[selected_col].isnull().sum()} ({current_df[selected_col].isnull().sum()/len(current_df)*100:.2f}%)
                """),
                mo.ui.plotly(fig)
            ])
    else:
        viz_content = mo.md("_Sélectionnez une colonne_")

    viz_content
    return dist, fig, selected_col, viz_content


@app.cell
def __(mo):
    mo.md(
        """
        ## 3️⃣ Suggestions de Processing

        Recommandations automatiques basées sur l'analyse des données pour chaque table.
        """
    )
    return


@app.cell
def __(DescriptiveAnalysis, bronze_tables, mo):
    # Créer des onglets pour les suggestions de chaque table
    suggestion_tabs_content = {}

    for table_name, table_df in bronze_tables.items():
        # Analyser chaque table
        table_analyzer = DescriptiveAnalysis(table_df, name=table_name)
        table_suggestions = table_analyzer.suggest_preprocessing()

        # Créer le contenu pour cette table
        table_sections = []

        if table_suggestions['missing_values']:
            table_sections.append(mo.md("#### 🔴 Valeurs Manquantes"))
            for sugg in table_suggestions['missing_values']:
                table_sections.append(mo.md(f"- {sugg}"))
            table_sections.append(mo.md(""))  # Espacement

        if table_suggestions['duplicates']:
            table_sections.append(mo.md("#### 🔁 Doublons"))
            for sugg in table_suggestions['duplicates']:
                table_sections.append(mo.md(f"- {sugg}"))
            table_sections.append(mo.md(""))

        if table_suggestions['data_types']:
            table_sections.append(mo.md("#### 🔧 Types de Données"))
            for sugg in table_suggestions['data_types']:
                table_sections.append(mo.md(f"- {sugg}"))
            table_sections.append(mo.md(""))

        if table_suggestions['outliers']:
            table_sections.append(mo.md("#### 📊 Outliers"))
            for sugg in table_suggestions['outliers']:
                table_sections.append(mo.md(f"- {sugg}"))
            table_sections.append(mo.md(""))

        if table_suggestions['normalization']:
            table_sections.append(mo.md("#### 📏 Normalisation"))
            for sugg in table_suggestions['normalization']:
                table_sections.append(mo.md(f"- {sugg}"))

        if not any(table_suggestions.values()):
            table_sections.append(mo.md("✅ **Aucune suggestion majeure** - Les données semblent en bon état !"))

        # Ajouter le contenu au dictionnaire des onglets
        suggestion_tabs_content[f"📋 {table_name}"] = mo.vstack(table_sections)

    # Créer les onglets
    suggestions_tabs = mo.ui.tabs(suggestion_tabs_content)
    suggestions_tabs
    return (
        sugg,
        suggestion_tabs_content,
        suggestions_tabs,
        table_analyzer,
        table_df,
        table_name,
        table_sections,
        table_suggestions,
    )


@app.cell
def __(mo):
    mo.md(f"""
    ## 📝 Résumé Global

    ### Tables disponibles dans Bronze:
    """)
    return


@app.cell
def __(bronze_tables, mo):
    summary_data = []
    for _table_name, df in bronze_tables.items():
        summary_data.append({
            'Table': _table_name,
            'Lignes': f"{len(df):,}",
            'Colonnes': len(df.columns),
            'Mémoire (MB)': f"{df.memory_usage(deep=True).sum() / 1024**2:.2f}"
        })

    summary_df = mo.ui.table(summary_data)
    summary_df
    return df, summary_data, summary_df


if __name__ == "__main__":
    app.run()
