"""Script de nettoyage et préparation des données (Bronze → Silver)."""

import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.cleaner import DataCleaner
from src.data.validator import DataValidator
from src.database.connection import DatabaseConnection
from src.database.writer import DatabaseWriter
from src.utils.config import config


def main():
    """Nettoie les données Bronze et les charge dans Silver."""
    print("="*60)
    print("NETTOYAGE DES DONNÉES (BRONZE → SILVER)")
    print("="*60)
    
    # Connexion à la base SQLite
    db_conn = DatabaseConnection(
        db_type=config.DB_TYPE,
        db_path=config.DB_PATH
    )
    
    with db_conn:
        conn = db_conn.connection
        writer = DatabaseWriter(db_conn)
        
        # Lister les tables Bronze
        tables_bronze = writer.get_table_list(schema='bronze')
        
        if tables_bronze.empty:
            print("❌ Aucune table Bronze trouvée!")
            print("Exécutez d'abord: uv run python scripts/load_to_db.py")
            return
        
        print(f"\n📊 {len(tables_bronze)} tables Bronze trouvées")
        
        cleaned_data = {}
        
        # Nettoyer chaque table Bronze
        for idx, row in tables_bronze.iterrows():
            full_name = row['name']
            table_name = full_name.replace('bronze_', '')
            
            print(f"\n{'='*60}")
            print(f"Traitement: {table_name}")
            print(f"{'='*60}")
            
            # Lire depuis Bronze
            df = pd.read_sql_query(f"SELECT * FROM {full_name}", conn)
            print(f"📥 Chargé depuis Bronze: {len(df):,} lignes")
            
            # Validation initiale
            validator = DataValidator(df, table_name)
            report = validator.generate_report()
            print(f"  - Valeurs manquantes: {len(report['missing_values'])} colonnes")
            print(f"  - Doublons: {report['duplicates']}")
            
            # Nettoyage
            cleaner = DataCleaner(df, table_name)
            cleaned_df = (
                cleaner
                .remove_duplicates()
                .handle_missing_values(strategy='drop', threshold=config.MISSING_THRESHOLD)
                .get_cleaned_data()
            )
            
            print(f"✓ Nettoyé: {len(df):,} → {len(cleaned_df):,} lignes")
            
            cleaned_data[table_name] = cleaned_df
        
        # Écrire dans Silver
        print(f"\n{'='*60}")
        print("📊 SILVER LAYER - Données nettoyées")
        print(f"{'='*60}")
        writer.write_multiple(cleaned_data, schema='silver', if_exists='replace')
        
        # Créer les indexes pour optimiser les requêtes
        print(f"\n{'='*60}")
        print("🔍 CRÉATION DES INDEXES")
        print(f"{'='*60}")
        
        cursor = conn.cursor()
        
        # Index pour olist_orders_dataset
        print("  - Indexes pour orders...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_order_id ON silver_olist_orders_dataset(order_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_customer_id ON silver_olist_orders_dataset(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_delivered_date ON silver_olist_orders_dataset(order_delivered_customer_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_estimated_date ON silver_olist_orders_dataset(order_estimated_delivery_date)")
        
        # Index pour olist_order_items_dataset
        print("  - Indexes pour order_items...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_items_order_id ON silver_olist_order_items_dataset(order_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_items_product_id ON silver_olist_order_items_dataset(product_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_items_seller_id ON silver_olist_order_items_dataset(seller_id)")
        
        # Index pour olist_customers_dataset
        print("  - Indexes pour customers...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_customers_customer_id ON silver_olist_customers_dataset(customer_id)")
        
        # Index pour olist_products_dataset
        print("  - Indexes pour products...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_products_product_id ON silver_olist_products_dataset(product_id)")
        
        # Index pour olist_sellers_dataset
        print("  - Indexes pour sellers...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_sellers_seller_id ON silver_olist_sellers_dataset(seller_id)")
        
        # Index pour olist_order_payments_dataset
        print("  - Indexes pour payments...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_payments_order_id ON silver_olist_order_payments_dataset(order_id)")
        
        # Index pour olist_order_reviews_dataset
        print("  - Indexes pour reviews...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_reviews_order_id ON silver_olist_order_reviews_dataset(order_id)")
        
        conn.commit()
        print("  ✓ 12 indexes créés")
    
    print(f"\n{'='*60}")
    print("✓ NETTOYAGE TERMINÉ (SILVER)")
    print(f"{'='*60}")
    print(f"Base de données: {config.DB_PATH}")
    print(f"Tables Silver créées: {len(cleaned_data)}")
    print(f"Indexes créés: 12")


if __name__ == "__main__":
    main()
