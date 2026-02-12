"""Écriture des données dans la base de données avec architecture Medallion."""

import pandas as pd
from typing import Dict, Optional, Literal
from .connection import DatabaseConnection


class DatabaseWriter:
    """Classe pour écrire des DataFrames dans SQLite avec architecture Medallion."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialise le writer.
        
        Args:
            db_connection: Instance de DatabaseConnection.
        """
        self.db_connection = db_connection
    
    def write_dataframe(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        schema: Literal['bronze', 'silver', 'gold'] = 'bronze',
        if_exists: str = 'replace',
        index: bool = False
    ) -> None:
        """
        Écrit un DataFrame dans une table avec préfixe de schéma.
        
        Args:
            df: DataFrame à écrire.
            table_name: Nom de la table (sans préfixe).
            schema: Schéma Medallion ('bronze', 'silver', 'gold').
            if_exists: Action si table existe ('fail', 'replace', 'append').
            index: Inclure l'index comme colonne.
        """
        # Préfixer le nom de table avec le schéma
        full_table_name = f"{schema}_{table_name}"
        
        if self.db_connection.db_type == 'sqlite':
            conn = self.db_connection.connection
            df.to_sql(
                full_table_name,
                conn,
                if_exists=if_exists,
                index=index
            )
            print(f"✓ Table '{full_table_name}' créée ({len(df)} lignes) [Schéma: {schema.upper()}]")
        
        elif self.db_connection.db_type == 'postgresql':
            df.to_sql(
                table_name, 
                self.db_connection.engine, 
                schema=schema,
                if_exists=if_exists, 
                index=index
            )
            print(f"✓ Table '{schema}.{table_name}' créée ({len(df)} lignes)")
    
    def write_multiple(
        self, 
        dataframes: Dict[str, pd.DataFrame],
        schema: Literal['bronze', 'silver', 'gold'] = 'bronze',
        if_exists: str = 'replace'
    ) -> None:
        """
        Écrit plusieurs DataFrames dans le même schéma.
        
        Args:
            dataframes: Dictionnaire {nom_table: DataFrame}.
            schema: Schéma Medallion cible.
            if_exists: Action si table existe.
        """
        print(f"\n{'='*60}")
        print(f"Écriture de {len(dataframes)} tables dans le schéma {schema.upper()}")
        print(f"{'='*60}")
        
        for table_name, df in dataframes.items():
            self.write_dataframe(df, table_name, schema=schema, if_exists=if_exists)
        
        print(f"\n✓ {len(dataframes)} tables écrites avec succès dans {schema.upper()}\n")
    
    def get_table_list(self, schema: Optional[str] = None) -> pd.DataFrame:
        """
        Liste toutes les tables de la base ou d'un schéma spécifique.
        
        Args:
            schema: Filtrer par schéma ('bronze', 'silver', 'gold') ou None pour tout.
        
        Returns:
            DataFrame avec les noms de tables.
        """
        if self.db_connection.db_type == 'sqlite':
            query = "SELECT name FROM sqlite_master WHERE type='table'"
            df = pd.read_sql_query(query, self.db_connection.connection)
            
            if schema:
                df = df[df['name'].str.startswith(f"{schema}_")]
            
            return df
        
        return pd.DataFrame()
