"""Script de chargement des données dans la base de données."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.loader import OlistDataLoader
from src.database.connection import DatabaseConnection
from src.database.writer import DatabaseWriter
from src.utils.config import config


def main():
    """Charge les données nettoyées dans DuckDB."""
    print("="*60)
    print("CHARGEMENT EN BASE DE DONNÉES")
    print("="*60)
    
    # Charger les données nettoyées
    loader = OlistDataLoader()
    
    # Charger depuis les fichiers cleaned ou bruts
    processed_files = list(config.PROCESSED_DATA_DIR.glob("*_cleaned.csv"))
    
    if processed_files:
        print(f"Chargement de {len(processed_files)} fichiers nettoyés...")
        dataframes = {}
        for file in processed_files:
            import pandas as pd
            name = file.stem.replace('_cleaned', '')
            dataframes[name] = pd.read_csv(file)
    else:
        print("Aucun fichier nettoyé trouvé, chargement des données brutes...")
        dataframes = loader.load_all()
    
    # Connexion à la base
    db_conn = DatabaseConnection(
        db_type=config.DB_TYPE,
        db_path=config.DB_PATH
    )
    
    with db_conn:
        # Écriture des données
        writer = DatabaseWriter(db_conn)
        writer.write_multiple(dataframes, if_exists='replace')
    
    print(f"\n{'='*60}")
    print("✓ DONNÉES CHARGÉES EN BASE")
    print(f"{'='*60}")
    print(f"Base de données: {config.DB_PATH}")
    print(f"Tables créées: {len(dataframes)}")


if __name__ == "__main__":
    main()
