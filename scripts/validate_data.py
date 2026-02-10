"""Script de validation des données avec Pandera.

Ce script montre comment utiliser Pandera pour valider la qualité
et la structure des datasets Olist avant analyse.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.loader import OlistDataLoader
from src.data.validator import DataValidator
from src.data.schemas import OLIST_SCHEMAS, get_schema


def main():
    """Valide tous les datasets Olist avec leurs schémas Pandera."""
    print("="*60)
    print("VALIDATION DES DONNÉES AVEC PANDERA")
    print("="*60)
    
    # Charger les données
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    
    print(f"\n✓ {len(dataframes)} fichiers chargés\n")
    
    # Valider chaque dataset
    results = []
    
    for name, df in dataframes.items():
        print(f"{'='*60}")
        print(f"Validation: {name}")
        print(f"{'='*60}")
        
        # Vérifier si un schéma existe
        if name not in OLIST_SCHEMAS:
            print(f"⚠️ Aucun schéma défini pour {name}, validation basique seulement")
            validator = DataValidator(df, name)
            report = validator.generate_report()
            results.append({
                'dataset': name,
                'schema_validation': 'N/A',
                'issues': len(report['issues'])
            })
            continue
        
        # Validation avec schéma Pandera
        validator = DataValidator(df, name)
        schema = get_schema(name)
        
        print(f"📋 Schéma: {len(schema.columns)} colonnes attendues")
        print(f"📊 Données: {df.shape[0]:,} lignes, {df.shape[1]} colonnes")
        
        # Valider
        is_valid = validator.validate_with_schema(schema)
        
        if is_valid:
            print(f"✅ Validation réussie!")
        else:
            print(f"❌ Validation échouée - Voir détails ci-dessus")
        
        # Rapport basique
        report = validator.generate_report()
        
        results.append({
            'dataset': name,
            'schema_validation': '✅' if is_valid else '❌',
            'duplicates': report['duplicates'],
            'missing_cols': len(report['missing_values']),
            'issues': len(report['issues'])
        })
        
        print()
    
    # Résumé global
    print(f"\n{'='*60}")
    print("RÉSUMÉ DE VALIDATION")
    print(f"{'='*60}")
    
    import pandas as pd
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    # Statistiques
    valid_count = sum(1 for r in results if r['schema_validation'] == '✅')
    print(f"\n✓ {valid_count}/{len(results)} datasets valides selon leurs schémas")


if __name__ == "__main__":
    main()
