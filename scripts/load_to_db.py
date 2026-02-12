"""Script de chargement des données dans SQLite (architecture Medallion)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.loader import OlistDataLoader
from src.database.connection import DatabaseConnection
from src.database.writer import DatabaseWriter
from src.utils.config import config


def main():
    """Charge les données dans SQLite avec architecture Bronze/Silver/Gold."""
    print("="*60)
    print("CHARGEMENT EN BASE DE DONNÉES SQLite")
    print("Architecture Medallion (Bronze/Silver/Gold)")
    print("="*60)
    
    # Charger les données depuis Kaggle
    print("\n📥 Chargement des données Kaggle...")
    loader = OlistDataLoader()
    dataframes = loader.load_all()
    
    print(f"\n✓ {len(dataframes)} fichiers chargés en mémoire")
    
    # Connexion à SQLite
    db_conn = DatabaseConnection(
        db_type=config.DB_TYPE,
        db_path=config.DB_PATH,
        create_schemas=True
    )
    
    with db_conn:
        writer = DatabaseWriter(db_conn)
        
        # BRONZE: Écriture des données brutes
        print(f"\n{'='*60}")
        print("📊 BRONZE LAYER - Données brutes")
        print(f"{'='*60}")
        writer.write_multiple(dataframes, schema='bronze', if_exists='replace')
        
        print(f"\n{'='*60}")
        print("✓ DONNÉES CHARGÉES EN BASE SQLite")
        print(f"{'='*60}")
        print(f"Base de données: {config.DB_PATH}")
        print(f"Tables Bronze créées: {len(dataframes)}")
        print(f"\nProchaines étapes:")
        print(f"  1. Nettoyage → SILVER layer (process_data.py)")
        print(f"  2. Agrégations → GOLD layer (analyses)")


if __name__ == "__main__":
    main()
