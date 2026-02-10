"""Prétraitement des données pour l'analyse."""

import pandas as pd
from typing import List, Optional


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les noms de colonnes (snake_case).
    
    Args:
        df: DataFrame source.
    
    Returns:
        DataFrame avec colonnes renommées.
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df


def parse_dates(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    """
    Convertit des colonnes en datetime.
    
    Args:
        df: DataFrame source.
        date_columns: Liste des colonnes à convertir.
    
    Returns:
        DataFrame avec dates converties.
    """
    df = df.copy()
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def extract_date_features(df: pd.DataFrame, date_col: str, prefix: Optional[str] = None) -> pd.DataFrame:
    """
    Extrait des features depuis une colonne datetime.
    
    Args:
        df: DataFrame source.
        date_col: Nom de la colonne datetime.
        prefix: Préfixe pour les nouvelles colonnes.
    
    Returns:
        DataFrame avec features temporelles ajoutées.
    """
    df = df.copy()
    prefix = prefix or date_col
    
    df[f'{prefix}_year'] = df[date_col].dt.year
    df[f'{prefix}_month'] = df[date_col].dt.month
    df[f'{prefix}_day'] = df[date_col].dt.day
    df[f'{prefix}_dayofweek'] = df[date_col].dt.dayofweek
    df[f'{prefix}_quarter'] = df[date_col].dt.quarter
    
    return df
