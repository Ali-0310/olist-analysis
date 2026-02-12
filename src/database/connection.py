"""Gestion des connexions à la base de données SQLite."""

import sqlite3
from sqlalchemy import create_engine
from pathlib import Path
from typing import Optional, Literal, List


class DatabaseConnection:
    """Classe pour gérer les connexions à SQLite avec architecture Medallion."""
    
    def __init__(
        self, 
        db_type: Literal['sqlite', 'postgresql'] = 'sqlite',
        db_path: Optional[Path] = None,
        connection_string: Optional[str] = None,
        create_schemas: bool = True
    ):
        """
        Initialise la connexion.
        
        Args:
            db_type: Type de base de données ('sqlite' ou 'postgresql').
            db_path: Chemin vers le fichier SQLite (si db_type='sqlite').
            connection_string: String de connexion SQLAlchemy (si db_type='postgresql').
            create_schemas: Créer automatiquement les schémas Bronze/Silver/Gold.
        """
        self.db_type = db_type
        self.db_path = db_path or Path("data/olist.db")
        self.connection_string = connection_string
        self.engine = None
        self.connection = None
        self.create_schemas = create_schemas
    
    def connect(self):
        """Établit la connexion à la base de données."""
        if self.db_type == 'sqlite':
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(str(self.db_path))
            print(f"✓ Connexion SQLite établie: {self.db_path}")
            
            # Créer les schémas Medallion si demandé
            if self.create_schemas:
                self._create_medallion_schemas()
        
        elif self.db_type == 'postgresql':
            if not self.connection_string:
                raise ValueError("Connection string requise pour PostgreSQL")
            self.engine = create_engine(self.connection_string)
            self.connection = self.engine.connect()
            print("✓ Connexion PostgreSQL établie")
        
        return self.connection
    
    def _create_medallion_schemas(self) -> None:
        """Crée les schémas Bronze, Silver et Gold pour l'architecture Medallion."""
        # Note: SQLite ne supporte pas les schémas natifs comme PostgreSQL
        # On utilise des préfixes de noms de tables : bronze_, silver_, gold_
        print("✓ Architecture Medallion configurée (préfixes: bronze_, silver_, gold_)")
    
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
