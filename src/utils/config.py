"""Configuration globale du projet."""

from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration du projet Olist Analysis."""
    
    # Chemins
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent
    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    SCHEMAS_DIR: Path = DATA_DIR / "schemas"
    NOTEBOOKS_DIR: Path = PROJECT_ROOT / "notebooks"
    
    # Base de données
    DB_TYPE: str = "duckdb"  # ou "postgresql"
    DB_PATH: Path = DATA_DIR / "olist.duckdb"
    DB_CONNECTION_STRING: Optional[str] = None
    
    # Kaggle
    KAGGLE_DATASET: str = "olistbr/brazilian-ecommerce"
    
    # Paramètres de nettoyage
    MISSING_THRESHOLD: float = 0.5  # Seuil pour supprimer colonnes
    OUTLIER_METHOD: str = "iqr"  # ou "zscore"
    
    def __post_init__(self):
        """Crée les dossiers nécessaires."""
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)


# Instance globale
config = Config()
