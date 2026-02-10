"""Écriture des données dans la base de données."""

import pandas as pd
from typing import Dict, Optional
from .connection import DatabaseConnection


class DatabaseWriter:
    """Classe pour écrire des DataFrames dans une base de données."""
    
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
        if_exists: str = 'replace',
        index: bool = False
    ) -> None:
        """
        Écrit un DataFrame dans une table.
        
        Args:
            df: DataFrame à écrire.
            table_name: Nom de la table.
            if_exists: Action si table existe ('fail', 'replace', 'append').
            index: Inclure l'index comme colonne.
        """
        if self.db_connection.db_type == 'duckdb':
            conn = self.db_connection.connection
            
            if if_exists == 'replace':
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            print(f"✓ Table '{table_name}' créée ({len(df)} lignes)")
        
        elif self.db_connection.db_type == 'postgresql':
            df.to_sql(
                table_name, 
                self.db_connection.engine, 
                if_exists=if_exists, 
                index=index
            )
            print(f"✓ Table '{table_name}' créée ({len(df)} lignes)")
    
    def write_multiple(
        self, 
        dataframes: Dict[str, pd.DataFrame],
        if_exists: str = 'replace'
    ) -> None:
        """
        Écrit plusieurs DataFrames.
        
        Args:
            dataframes: Dictionnaire {nom_table: DataFrame}.
            if_exists: Action si table existe.
        """
        print(f"\nÉcriture de {len(dataframes)} tables...")
        for table_name, df in dataframes.items():
            self.write_dataframe(df, table_name, if_exists=if_exists)
        print(f"✓ {len(dataframes)} tables écrites avec succès\n")
