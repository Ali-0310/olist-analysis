"""Classes de nettoyage de données (POO)."""

import pandas as pd
from typing import List, Dict, Optional, Literal


class DataCleaner:
    """Classe de base pour le nettoyage de données."""
    
    def __init__(self, df: pd.DataFrame, name: str = "dataset"):
        """
        Initialise le nettoyeur de données.
        
        Args:
            df: DataFrame à nettoyer.
            name: Nom du dataset pour les logs.
        """
        self.df = df.copy()
        self.name = name
        self.original_shape = df.shape
        self.cleaning_log: List[str] = []
    
    def remove_duplicates(self, subset: Optional[List[str]] = None) -> 'DataCleaner':
        """
        Supprime les lignes dupliquées.
        
        Args:
            subset: Liste de colonnes à considérer pour les doublons.
        
        Returns:
            Self pour chaînage.
        """
        before = len(self.df)
        self.df = self.df.drop_duplicates(subset=subset)
        removed = before - len(self.df)
        
        if removed > 0:
            msg = f"Supprimé {removed} doublons"
            if subset:
                msg += f" (colonnes: {', '.join(subset)})"
            self.cleaning_log.append(msg)
        
        return self
    
    def handle_missing_values(
        self, 
        strategy: Literal['drop', 'fill', 'none'] = 'none',
        fill_value: Optional[Dict[str, any]] = None,
        threshold: float = 0.5
    ) -> 'DataCleaner':
        """
        Gère les valeurs manquantes.
        
        Args:
            strategy: 'drop' (supprimer), 'fill' (remplir), 'none' (aucune action).
            fill_value: Dictionnaire {colonne: valeur} pour remplissage.
            threshold: Proportion max de valeurs manquantes pour conserver colonne.
        
        Returns:
            Self pour chaînage.
        """
        if strategy == 'none':
            return self
        
        missing_before = self.df.isnull().sum().sum()
        
        if strategy == 'drop':
            # Supprimer colonnes avec trop de valeurs manquantes
            cols_to_drop = []
            for col in self.df.columns:
                missing_ratio = self.df[col].isnull().sum() / len(self.df)
                if missing_ratio > threshold:
                    cols_to_drop.append(col)
            
            if cols_to_drop:
                self.df = self.df.drop(columns=cols_to_drop)
                self.cleaning_log.append(
                    f"Supprimé {len(cols_to_drop)} colonnes avec >{threshold*100}% de valeurs manquantes"
                )
            
            # Supprimer lignes restantes avec valeurs manquantes
            before = len(self.df)
            self.df = self.df.dropna()
            removed = before - len(self.df)
            if removed > 0:
                self.cleaning_log.append(f"Supprimé {removed} lignes avec valeurs manquantes")
        
        elif strategy == 'fill':
            if fill_value:
                self.df = self.df.fillna(fill_value)
                self.cleaning_log.append(f"Rempli {missing_before} valeurs manquantes")
        
        return self
    
    def convert_dtypes(self, dtype_map: Dict[str, str]) -> 'DataCleaner':
        """
        Convertit les types de données des colonnes.
        
        Args:
            dtype_map: Dictionnaire {colonne: type} (ex: {'date': 'datetime64'}).
        
        Returns:
            Self pour chaînage.
        """
        for col, dtype in dtype_map.items():
            if col not in self.df.columns:
                continue
            
            try:
                if dtype.startswith('datetime'):
                    self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                else:
                    self.df[col] = self.df[col].astype(dtype)
                self.cleaning_log.append(f"Converti '{col}' en {dtype}")
            except Exception as e:
                self.cleaning_log.append(f"Erreur conversion '{col}': {e}")
        
        return self
    
    def remove_outliers(
        self, 
        columns: List[str], 
        method: Literal['iqr', 'zscore'] = 'iqr',
        threshold: float = 3.0
    ) -> 'DataCleaner':
        """
        Supprime les valeurs aberrantes.
        
        Args:
            columns: Colonnes numériques à analyser.
            method: 'iqr' (intervalle interquartile) ou 'zscore' (écart-type).
            threshold: Seuil pour la méthode (1.5 pour IQR, 3.0 pour zscore).
        
        Returns:
            Self pour chaînage.
        """
        before = len(self.df)
        
        for col in columns:
            if col not in self.df.columns or not pd.api.types.is_numeric_dtype(self.df[col]):
                continue
            
            if method == 'iqr':
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - threshold * IQR
                upper = Q3 + threshold * IQR
                self.df = self.df[(self.df[col] >= lower) & (self.df[col] <= upper)]
            
            elif method == 'zscore':
                from scipy import stats
                z_scores = stats.zscore(self.df[col].dropna())
                self.df = self.df[abs(z_scores) < threshold]
        
        removed = before - len(self.df)
        if removed > 0:
            self.cleaning_log.append(
                f"Supprimé {removed} outliers (méthode: {method})"
            )
        
        return self
    
    def validate(self) -> bool:
        """
        Valide que les données nettoyées sont cohérentes.
        
        Returns:
            True si validation réussie.
        """
        # Vérifications basiques
        if self.df.empty:
            self.cleaning_log.append("⚠️ ERREUR: DataFrame vide après nettoyage")
            return False
        
        if self.df.shape[0] < self.original_shape[0] * 0.1:
            self.cleaning_log.append(
                f"⚠️ ATTENTION: >90% des données supprimées "
                f"({self.original_shape[0]} → {self.df.shape[0]})"
            )
        
        return True
    
    def get_cleaned_data(self) -> pd.DataFrame:
        """Retourne le DataFrame nettoyé."""
        return self.df
    
    def print_summary(self) -> None:
        """Affiche un résumé du nettoyage."""
        print(f"\n{'='*60}")
        print(f"Résumé du nettoyage: {self.name}")
        print(f"{'='*60}")
        print(f"Shape originale: {self.original_shape}")
        print(f"Shape finale:    {self.df.shape}")
        print(f"\nOpérations effectuées:")
        for i, log in enumerate(self.cleaning_log, 1):
            print(f"  {i}. {log}")
        print(f"{'='*60}\n")
