"""Analyses descriptives des données."""

import pandas as pd
import numpy as np
from typing import Dict, List


class DescriptiveAnalysis:
    """Classe pour les analyses statistiques descriptives."""
    
    def __init__(self, df: pd.DataFrame, name: str = "dataset"):
        """
        Initialise l'analyseur.
        
        Args:
            df: DataFrame à analyser.
            name: Nom du dataset.
        """
        self.df = df
        self.name = name
    
    def basic_stats(self) -> pd.DataFrame:
        """
        Statistiques descriptives de base.
        
        Returns:
            DataFrame avec statistiques pour colonnes numériques.
        """
        return self.df.describe()
    
    def column_info(self) -> pd.DataFrame:
        """
        Informations détaillées sur chaque colonne.
        
        Returns:
            DataFrame avec type, valeurs uniques, manquantes, etc.
        """
        info_data = []
        for col in self.df.columns:
            info_data.append({
                'Colonne': col,
                'Type': str(self.df[col].dtype),
                'Non-null': self.df[col].count(),
                'Null': self.df[col].isnull().sum(),
                'Null %': round(self.df[col].isnull().sum() / len(self.df) * 100, 2),
                'Uniques': self.df[col].nunique(),
                'Exemple': str(self.df[col].iloc[0]) if len(self.df) > 0 else None
            })
        return pd.DataFrame(info_data)
    
    def detect_patterns(self) -> Dict[str, List[str]]:
        """
        Détecte des patterns dans les données.
        
        Returns:
            Dict avec catégories de colonnes détectées.
        """
        patterns = {
            'id_columns': [],
            'date_columns': [],
            'categorical': [],
            'numerical': [],
            'text': []
        }
        
        for col in self.df.columns:
            col_lower = col.lower()
            
            # IDs
            if 'id' in col_lower or col_lower.endswith('_key'):
                patterns['id_columns'].append(col)
            
            # Dates
            elif 'date' in col_lower or 'time' in col_lower or 'timestamp' in col_lower:
                patterns['date_columns'].append(col)
            
            # Numériques
            elif pd.api.types.is_numeric_dtype(self.df[col]):
                patterns['numerical'].append(col)
            
            # Catégorielles (peu de valeurs uniques)
            elif self.df[col].nunique() < 50:
                patterns['categorical'].append(col)
            
            # Texte
            else:
                patterns['text'].append(col)
        
        return patterns
    
    def correlation_matrix(self) -> pd.DataFrame:
        """
        Matrice de corrélation pour colonnes numériques.
        
        Returns:
            DataFrame de corrélation.
        """
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) == 0:
            return pd.DataFrame()
        return self.df[numeric_cols].corr()
