"""Script de nettoyage et préparation des données."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.loader import OlistDataLoader
from src.data.cleaner import DataCleaner
from src.data.validator import DataValidator
from src.utils.config import config


def main():
    """Nettoie les données Olist et les sauvegarde."""
    print("="*60)
    print("NETTOYAGE DES DONNÉES OLIST")
    print("="*60)
    
    # Charger les données
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    
    # Nettoyer chaque dataset
    cleaned_data = {}
    
    for name, df in dataframes.items():
        print(f"\n{'='*60}")
        print(f"Traitement: {name}")
        print(f"{'='*60}")
        
        # Validation initiale
        validator = DataValidator(df, name)
        report = validator.generate_report()
        print(f"Shape: {report['shape']}")
        print(f"Mémoire: {report['memory_mb']} MB")
        
        # Nettoyage
        cleaner = DataCleaner(df, name)
        cleaned_df = (
            cleaner
            .remove_duplicates()
            .handle_missing_values(strategy='drop', threshold=config.MISSING_THRESHOLD)
            .get_cleaned_data()
        )
        
        cleaner.print_summary()
        
        # Sauvegarder
        output_path = config.PROCESSED_DATA_DIR / f"{name}_cleaned.csv"
        cleaned_df.to_csv(output_path, index=False)
        print(f"✓ Sauvegardé: {output_path}")
        
        cleaned_data[name] = cleaned_df
    
    print(f"\n{'='*60}")
    print("✓ NETTOYAGE TERMINÉ")
    print(f"{'='*60}")
    print(f"Fichiers sauvegardés dans: {config.PROCESSED_DATA_DIR}")


if __name__ == "__main__":
    main()
