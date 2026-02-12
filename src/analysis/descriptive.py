"""Analyses descriptives des données."""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


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
            null_count = self.df[col].isnull().sum()
            null_pct = round(null_count / len(self.df) * 100, 2)
            unique_count = self.df[col].nunique()
            
            info_data.append({
                'Colonne': col,
                'Type': str(self.df[col].dtype),
                'Non-null': self.df[col].count(),
                'Null': null_count,
                'Null %': null_pct,
                'Uniques': unique_count,
                'Exemple': str(self.df[col].iloc[0]) if len(self.df) > 0 else None
            })
        return pd.DataFrame(info_data)
    
    def get_top_values(self, column: str, top_n: int = 3) -> pd.DataFrame:
        """
        Retourne les valeurs les plus fréquentes d'une colonne.
        
        Args:
            column: Nom de la colonne.
            top_n: Nombre de valeurs à retourner.
            
        Returns:
            DataFrame avec valeurs et leurs fréquences.
        """
        value_counts = self.df[column].value_counts().head(top_n)
        return pd.DataFrame({
            'Valeur': value_counts.index,
            'Count': value_counts.values,
            'Pourcentage': np.round(value_counts.values / len(self.df) * 100, 2)
        })
    
    def value_distribution(self, column: str, max_categories: int = 50) -> pd.DataFrame:
        """
        Distribution complète des valeurs d'une colonne.
        
        Args:
            column: Nom de la colonne.
            max_categories: Nombre maximum de catégories à retourner.
            
        Returns:
            DataFrame avec distribution des valeurs.
        """
        value_counts = self.df[column].value_counts().head(max_categories)
        return pd.DataFrame({
            'Valeur': value_counts.index,
            'Count': value_counts.values,
            'Pourcentage': np.round(value_counts.values / len(self.df) * 100, 2)
        })
    
    def suggest_preprocessing(self) -> Dict[str, List[str]]:
        """
        Génère des suggestions de preprocessing basées sur l'analyse.
        
        Returns:
            Dict avec catégories de suggestions.
        """
        suggestions = {
            'missing_values': [],
            'duplicates': [],
            'data_types': [],
            'outliers': [],
            'normalization': []
        }
        
        # Vérifier les valeurs manquantes
        for col in self.df.columns:
            null_pct = self.df[col].isnull().sum() / len(self.df) * 100
            if null_pct > 50:
                suggestions['missing_values'].append(
                    f"⚠️ {col}: {null_pct:.1f}% null - Considérer suppression de la colonne"
                )
            elif null_pct > 5:
                suggestions['missing_values'].append(
                    f"📝 {col}: {null_pct:.1f}% null - Imputation recommandée"
                )
        
        # Vérifier les doublons
        dup_count = self.df.duplicated().sum()
        if dup_count > 0:
            suggestions['duplicates'].append(
                f"🔁 {dup_count} lignes dupliquées ({dup_count/len(self.df)*100:.2f}%) - Suppression recommandée"
            )
        
        # Vérifier les types de données
        for col in self.df.columns:
            col_lower = col.lower()
            if ('date' in col_lower or 'time' in col_lower) and self.df[col].dtype == 'object':
                suggestions['data_types'].append(
                    f"📅 {col}: Convertir en datetime"
                )
            elif 'price' in col_lower or 'amount' in col_lower or 'value' in col_lower:
                if self.df[col].dtype == 'object':
                    suggestions['data_types'].append(
                        f"💰 {col}: Convertir en numeric"
                    )
        
        # Vérifier les outliers (colonnes numériques)
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            Q1 = self.df[col].quantile(0.25)
            Q3 = self.df[col].quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((self.df[col] < (Q1 - 1.5 * IQR)) | (self.df[col] > (Q3 + 1.5 * IQR))).sum()
            if outliers > 0:
                outlier_pct = outliers / len(self.df) * 100
                if outlier_pct > 5:
                    suggestions['outliers'].append(
                        f"📊 {col}: {outliers} outliers ({outlier_pct:.2f}%) - Vérifier et traiter"
                    )
        
        # Suggestions de normalisation
        for col in numeric_cols:
            if self.df[col].std() > self.df[col].mean() * 2:
                suggestions['normalization'].append(
                    f"📏 {col}: Distribution asymétrique - Normalisation recommandée"
                )
        
        return suggestions
    
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
