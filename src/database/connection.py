"""Gestion des connexions à la base de données."""

import duckdb
from sqlalchemy import create_engine
from pathlib import Path
from typing import Optional, Literal


class DatabaseConnection:
    """Classe pour gérer les connexions aux bases de données."""
    
    def __init__(
        self, 
        db_type: Literal['duckdb', 'postgresql'] = 'duckdb',
        db_path: Optional[Path] = None,
        connection_string: Optional[str] = None
    ):
        """
        Initialise la connexion.
        
        Args:
            db_type: Type de base de données ('duckdb' ou 'postgresql').
            db_path: Chemin vers le fichier DuckDB (si db_type='duckdb').
            connection_string: String de connexion SQLAlchemy (si db_type='postgresql').
        """
        self.db_type = db_type
        self.db_path = db_path or Path("data/olist.duckdb")
        self.connection_string = connection_string
        self.engine = None
        self.connection = None
    
    def connect(self):
        """Établit la connexion à la base de données."""
        if self.db_type == 'duckdb':
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.connection = duckdb.connect(str(self.db_path))
            print(f"✓ Connexion DuckDB établie: {self.db_path}")
        
        elif self.db_type == 'postgresql':
            if not self.connection_string:
                raise ValueError("Connection string requise pour PostgreSQL")
            self.engine = create_engine(self.connection_string)
            self.connection = self.engine.connect()
            print("✓ Connexion PostgreSQL établie")
        
        return self.connection
    
    def close(self):
        """Ferme la connexion."""
        if self.connection:
            self.connection.close()
            print("✓ Connexion fermée")
    
    def __enter__(self):
        """Context manager: entrée."""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: sortie."""
        self.close()
