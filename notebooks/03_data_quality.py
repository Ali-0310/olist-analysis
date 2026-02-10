"""Notebook Marimo - Qualité des données."""

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
    
    sys.path.insert(0, str(Path.cwd().parent))
    
    from src.data.loader import OlistDataLoader
    from src.data.validator import DataValidator
    
    mo.md("# ✅ Qualité des Données - Olist")
    return mo, pd, px, sys, Path, OlistDataLoader, DataValidator


@app.cell
def __(mo, OlistDataLoader):
    mo.md("## Chargement des données")
    
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    
    mo.md(f"✓ {len(dataframes)} fichiers chargés")
    return loader, dataframes


@app.cell
def __(mo, dataframes, DataValidator):
    mo.md("## 📋 Rapport de qualité global")
    
    quality_reports = {}
    
    for name, df in dataframes.items():
        validator = DataValidator(df, name)
        quality_reports[name] = validator.generate_report()
    
    mo.md(f"Validation de {len(quality_reports)} fichiers")
    return quality_reports, validator


@app.cell
def __(mo, quality_reports, pd):
    # Résumé global
    summary_data = []
    for name, report in quality_reports.items():
        summary_data.append({
            'Fichier': name,
            'Lignes': report['shape'][0],
            'Colonnes': report['shape'][1],
            'Mémoire (MB)': report['memory_mb'],
            'Valeurs manquantes': len(report['missing_values']),
            'Doublons': report['duplicates'],
            'Problèmes': len(report['issues'])
        })
    
    summary_df = pd.DataFrame(summary_data)
    mo.ui.table(summary_df)
    return summary_data, summary_df


@app.cell
def __(mo, quality_reports):
    # Sélecteur de fichier
    file_selector = mo.ui.dropdown(
        options=list(quality_reports.keys()),
        value=list(quality_reports.keys())[0] if quality_reports else None,
        label="📁 Voir détails:"
    )
    file_selector
    return file_selector,


@app.cell
def __(mo, quality_reports, file_selector):
    if file_selector.value:
        report = quality_reports[file_selector.value]
        
        mo.md(f"""
        ## 🔍 Détails: {file_selector.value}
        
        ### Valeurs manquantes
        """)
        
        if report['missing_values']:
            missing_df = pd.DataFrame([
                {'Colonne': k, 'Proportion manquante': f"{v*100:.2f}%"}
                for k, v in report['missing_values'].items()
            ])
            mo.ui.table(missing_df)
        else:
            mo.md("✓ Aucune valeur manquante")
    return report,


@app.cell
def __(mo, report):
    if 'report' in locals():
        mo.md("### ⚠️ Problèmes détectés")
        
        if report['issues']:
            for issue in report['issues']:
                mo.md(f"- {issue}")
        else:
            mo.md("✓ Aucun problème majeur détecté")
    return


@app.cell
def __(mo, px, summary_df):
    # Graphiques de qualité
    mo.md("## 📊 Visualisations")
    
    fig = px.bar(
        summary_df,
        x='Fichier',
        y='Problèmes',
        title='Nombre de problèmes par fichier',
        color='Problèmes',
        color_continuous_scale='Reds'
    )
    mo.ui.plotly(fig)
    return fig,


if __name__ == "__main__":
    app.run()
