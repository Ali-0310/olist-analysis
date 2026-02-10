"""Validation de la qualité des données avec Pandera."""

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from typing import Dict, List, Optional


class DataValidator:
    """Classe pour valider la qualité des données."""
    
    def __init__(self, df: pd.DataFrame, name: str = "dataset"):
        """
        Initialise le validateur.
        
        Args:
            df: DataFrame à valider.
            name: Nom du dataset.
        """
        self.df = df
        self.name = name
        self.issues: List[str] = []
    
    def check_missing_values(self, threshold: float = 0.1) -> Dict[str, float]:
        """
        Vérifie les valeurs manquantes.
        
        Args:
            threshold: Seuil d'alerte (proportion).
        
        Returns:
            Dict avec colonnes et proportions de valeurs manquantes.
        """
        missing = self.df.isnull().sum()
        missing_pct = missing / len(self.df)
        
        problematic = missing_pct[missing_pct > threshold]
        if not problematic.empty:
            self.issues.append(
                f"{len(problematic)} colonnes avec >{threshold*100}% de valeurs manquantes"
            )
        
        return missing_pct[missing_pct > 0].to_dict()
    
    def check_duplicates(self, subset: Optional[List[str]] = None) -> int:
        """
        Vérifie les doublons.
        
        Args:
            subset: Colonnes à considérer.
        
        Returns:
            Nombre de doublons.
        """
        duplicates = self.df.duplicated(subset=subset).sum()
        if duplicates > 0:
            self.issues.append(f"{duplicates} lignes dupliquées détectées")
        return duplicates
    
    def check_data_types(self) -> pd.DataFrame:
        """
        Analyse les types de données.
        
        Returns:
            DataFrame avec types et exemples de valeurs.
        """
        type_info = pd.DataFrame({
            'Colonne': self.df.columns,
            'Type': self.df.dtypes.values,
            'Exemple': [self.df[col].iloc[0] if len(self.df) > 0 else None 
                       for col in self.df.columns]
        })
        return type_info
    
    def generate_report(self) -> Dict:
        """
        Génère un rapport de validation complet.
        
        Returns:
            Dictionnaire avec toutes les métriques de qualité.
        """
        report = {
            'name': self.name,
            'shape': self.df.shape,
            'memory_mb': round(self.df.memory_usage(deep=True).sum() / 1024**2, 2),
            'missing_values': self.check_missing_values(),
            'duplicates': self.check_duplicates(),
            'dtypes': self.df.dtypes.value_counts().to_dict(),
            'issues': self.issues
        }
        return report
    
    def validate_with_schema(self, schema: DataFrameSchema) -> bool:
        """
        Valide le DataFrame avec un schéma Pandera.
        
        Args:
            schema: Schéma Pandera à appliquer.
        
        Returns:
            True si validation réussie, False sinon.
        """
        try:
            schema.validate(self.df, lazy=True)
            return True
        except pa.errors.SchemaErrors as err:
            self.issues.append(f"Erreurs de schéma: {len(err.failure_cases)} violations")
            # Afficher les erreurs
            print(f"\n⚠️ Erreurs de validation pour {self.name}:")
            print(err.failure_cases)
            return False


# Exemples de schémas Pandera réutilisables

def create_orders_schema() -> DataFrameSchema:
    """
    Schéma de validation pour la table des commandes.
    
    Returns:
        DataFrameSchema pour orders.
    """
    return DataFrameSchema({
        "order_id": Column(str, nullable=False, unique=True),
        "customer_id": Column(str, nullable=False),
        "order_status": Column(str, nullable=False, 
                              checks=Check.isin(['delivered', 'shipped', 'processing', 'canceled'])),
        "order_purchase_timestamp": Column("datetime64[ns]", nullable=False),
        "order_approved_at": Column("datetime64[ns]", nullable=True),
        "order_delivered_customer_date": Column("datetime64[ns]", nullable=True),
    })


def create_products_schema() -> DataFrameSchema:
    """
    Schéma de validation pour la table des produits.
    
    Returns:
        DataFrameSchema pour products.
    """
    return DataFrameSchema({
        "product_id": Column(str, nullable=False, unique=True),
        "product_category_name": Column(str, nullable=True),
        "product_weight_g": Column(float, nullable=True, checks=Check.greater_than(0)),
        "product_length_cm": Column(float, nullable=True, checks=Check.greater_than(0)),
        "product_height_cm": Column(float, nullable=True, checks=Check.greater_than(0)),
        "product_width_cm": Column(float, nullable=True, checks=Check.greater_than(0)),
    })


def create_payments_schema() -> DataFrameSchema:
    """
    Schéma de validation pour la table des paiements.
    
    Returns:
        DataFrameSchema pour payments.
    """
    return DataFrameSchema({
        "order_id": Column(str, nullable=False),
        "payment_sequential": Column(int, nullable=False, checks=Check.greater_than_or_equal_to(1)),
        "payment_type": Column(str, nullable=False,
                              checks=Check.isin(['credit_card', 'boleto', 'voucher', 'debit_card'])),
        "payment_installments": Column(int, nullable=False, checks=Check.in_range(1, 24)),
        "payment_value": Column(float, nullable=False, checks=Check.greater_than(0)),
    })
