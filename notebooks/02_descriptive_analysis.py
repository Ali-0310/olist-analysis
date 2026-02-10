"""Notebook Marimo - Analyses descriptives détaillées."""

import marimo

__generated_with = "0.1.0"
app = marimo.App()


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import sys
    from pathlib import Path
    
    sys.path.insert(0, str(Path.cwd().parent))
    
    from src.data.loader import OlistDataLoader
    from src.analysis.descriptive import DescriptiveAnalysis
    
    mo.md("# 📊 Analyses Descriptives - Olist Dataset")
    return mo, pd, px, go, sys, Path, OlistDataLoader, DescriptiveAnalysis


@app.cell
def __(mo, OlistDataLoader):
    mo.md("## Chargement des données")
    
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    
    mo.md(f"✓ {len(dataframes)} fichiers chargés")
    return loader, dataframes


@app.cell
def __(mo, dataframes):
    # Sélecteur de fichier
    file_select = mo.ui.dropdown(
        options=list(dataframes.keys()),
        value=list(dataframes.keys())[0] if dataframes else None,
        label="📁 Sélectionner un fichier:"
    )
    file_select
    return file_select,


@app.cell
def __(mo, dataframes, file_select, DescriptiveAnalysis):
    if file_select.value:
        df = dataframes[file_select.value]
        analyzer = DescriptiveAnalysis(df, name=file_select.value)
        
        mo.md(f"## 🔍 Analyse: {file_select.value}")
    return df, analyzer


@app.cell
def __(mo, analyzer):
    if 'analyzer' in locals():
        mo.md("### Statistiques descriptives")
        stats = analyzer.basic_stats()
        mo.ui.table(stats)
    return stats,


@app.cell
def __(mo, analyzer):
    if 'analyzer' in locals():
        mo.md("### Informations par colonne")
        col_info = analyzer.column_info()
        mo.ui.table(col_info)
    return col_info,


@app.cell
def __(mo, analyzer):
    if 'analyzer' in locals():
        mo.md("### Détection de patterns")
        patterns = analyzer.detect_patterns()
        
        mo.md(f"""
        - **Colonnes ID**: {len(patterns['id_columns'])}
        - **Colonnes dates**: {len(patterns['date_columns'])}
        - **Colonnes numériques**: {len(patterns['numerical'])}
        - **Colonnes catégorielles**: {len(patterns['categorical'])}
        - **Colonnes texte**: {len(patterns['text'])}
        """)
    return patterns,


@app.cell
def __(mo, analyzer, px, df):
    if 'analyzer' in locals():
        # Graphique de corrélation si données numériques
        corr = analyzer.correlation_matrix()
        
        if not corr.empty:
            mo.md("### 🔗 Matrice de corrélation")
            fig = px.imshow(
                corr,
                text_auto='.2f',
                aspect="auto",
                color_continuous_scale='RdBu_r',
                title=f"Corrélations - {file_select.value}"
            )
            mo.ui.plotly(fig)
    return corr,


if __name__ == "__main__":
    app.run()
