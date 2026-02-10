"""Chargement des données Olist depuis Kaggle."""

import kagglehub
import pandas as pd
from pathlib import Path
from typing import Dict, Optional


class OlistDataLoader:
    """Classe pour télécharger et charger le dataset Olist depuis Kaggle."""
    
    DATASET_NAME = "olistbr/brazilian-ecommerce"
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Initialise le chargeur de données.
        
        Args:
            cache_dir: Répertoire de cache pour les données téléchargées.
                      Par défaut, utilise le cache de kagglehub.
        """
        self.cache_dir = cache_dir
        self.data_path: Optional[Path] = None
        self.dataframes: Dict[str, pd.DataFrame] = {}
    
    def download(self) -> Path:
        """
        Télécharge le dataset Olist depuis Kaggle.
        
        Returns:
            Path: Chemin vers le répertoire contenant les fichiers CSV.
        """
        print(f"Téléchargement du dataset {self.DATASET_NAME}...")
        self.data_path = Path(kagglehub.dataset_download(self.DATASET_NAME))
        print(f"Dataset téléchargé dans: {self.data_path}")
        return self.data_path
    
    def load_all(self) -> Dict[str, pd.DataFrame]:
        """
        Charge tous les fichiers CSV du dataset.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionnaire {nom_fichier: DataFrame}.
        """
        if self.data_path is None:
            self.download()
        
        csv_files = list(self.data_path.glob("*.csv"))
        print(f"Chargement de {len(csv_files)} fichiers CSV...")
        
        for csv_file in csv_files:
            name = csv_file.stem
            print(f"  - Chargement de {csv_file.name}...")
            self.dataframes[name] = pd.read_csv(csv_file)
        
        print(f"✓ {len(self.dataframes)} fichiers chargés")
        return self.dataframes
    
    def load_file(self, filename: str) -> pd.DataFrame:
        """
        Charge un fichier CSV spécifique.
        
        Args:
            filename: Nom du fichier (avec ou sans .csv).
        
        Returns:
            pd.DataFrame: DataFrame chargé.
        """
        if self.data_path is None:
            self.download()
        
        if not filename.endswith('.csv'):
            filename = f"{filename}.csv"
        
        filepath = self.data_path / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Fichier non trouvé: {filepath}")
        
        df = pd.read_csv(filepath)
        name = filepath.stem
        self.dataframes[name] = df
        return df
    
    def get_dataframe(self, name: str) -> Optional[pd.DataFrame]:
        """
        Récupère un DataFrame déjà chargé.
        
        Args:
            name: Nom du fichier (sans extension).
        
        Returns:
            Optional[pd.DataFrame]: DataFrame ou None si non chargé.
        """
        return self.dataframes.get(name)
    
    def summary(self) -> pd.DataFrame:
        """
        Génère un résumé des datasets chargés.
        
        Returns:
            pd.DataFrame: Résumé avec nom, lignes, colonnes, mémoire.
        """
        if not self.dataframes:
            return pd.DataFrame()
        
        summary_data = []
        for name, df in self.dataframes.items():
            summary_data.append({
                'Fichier': name,
                'Lignes': len(df),
                'Colonnes': len(df.columns),
                'Mémoire (MB)': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
            })
        
        return pd.DataFrame(summary_data)
