"""Notebook Marimo - Exploration initiale du dataset Olist."""

import marimo

__generated_with = "0.1.0"
app = marimo.App()


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
    
    mo.md("# 🛒 Exploration du Dataset Olist")
    return mo, pd, px, sys, Path, OlistDataLoader


@app.cell
def __(mo, OlistDataLoader):
    mo.md("## 1. Chargement des données")
    
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    
    mo.md(f"**{len(dataframes)} fichiers chargés** depuis Kaggle")
    return loader, dataframes


@app.cell
def __(mo, loader):
    mo.md("## 2. Résumé des fichiers")
    
    summary = loader.summary()
    mo.ui.table(summary)
    return summary,


@app.cell
def __(mo, dataframes):
    mo.md("## 3. Sélectionner un fichier à explorer")
    
    file_selector = mo.ui.dropdown(
        options=list(dataframes.keys()),
        value=list(dataframes.keys())[0] if dataframes else None,
        label="Choisir un fichier:"
    )
    file_selector
    return file_selector,


@app.cell
def __(mo, dataframes, file_selector):
    if file_selector.value:
        selected_df = dataframes[file_selector.value]
        
        mo.md(f"""
        ### 📊 Aperçu: {file_selector.value}
        
        - **Lignes**: {len(selected_df):,}
        - **Colonnes**: {len(selected_df.columns)}
        - **Mémoire**: {selected_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB
        """)
    return selected_df,


@app.cell
def __(mo, selected_df):
    if 'selected_df' in locals():
        mo.md("#### Premières lignes")
        mo.ui.table(selected_df.head(10))
    return


@app.cell
def __(mo, selected_df):
    if 'selected_df' in locals():
        mo.md("#### Informations colonnes")
        
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
        mo.ui.table(info_df)
    return


if __name__ == "__main__":
    app.run()
