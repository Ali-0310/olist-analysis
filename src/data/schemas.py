"""Schémas de validation Pandera pour les datasets Olist.

Ce module contient les schémas de validation réutilisables pour chaque table
du dataset Olist. Les schémas définissent les types, contraintes et validations
pour assurer la qualité des données.

Usage:
    from src.data.schemas import OLIST_SCHEMAS
    from src.data.validator import DataValidator
    
    validator = DataValidator(df, name="orders")
    validator.validate_with_schema(OLIST_SCHEMAS['orders'])
"""

import pandera as pa
from pandera import Column, DataFrameSchema, Check


# Schéma pour olist_orders_dataset
ORDERS_SCHEMA = DataFrameSchema(
    {
        "order_id": Column(str, nullable=False, unique=True, 
                          description="Identifiant unique de la commande"),
        "customer_id": Column(str, nullable=False,
                             description="Identifiant du client"),
        "order_status": Column(str, nullable=False,
                              checks=Check.isin(['delivered', 'shipped', 'processing', 
                                                'canceled', 'invoiced', 'created',
                                                'approved', 'unavailable']),
                              description="Statut de la commande"),
        "order_purchase_timestamp": Column("datetime64[ns]", nullable=False,
                                          description="Date d'achat"),
        "order_approved_at": Column("datetime64[ns]", nullable=True,
                                   description="Date d'approbation"),
        "order_delivered_carrier_date": Column("datetime64[ns]", nullable=True,
                                               description="Date de livraison au transporteur"),
        "order_delivered_customer_date": Column("datetime64[ns]", nullable=True,
                                                description="Date de livraison au client"),
        "order_estimated_delivery_date": Column("datetime64[ns]", nullable=True,
                                                description="Date de livraison estimée"),
    },
    name="orders",
    description="Commandes Olist"
)


# Schéma pour olist_customers_dataset
CUSTOMERS_SCHEMA = DataFrameSchema(
    {
        "customer_id": Column(str, nullable=False, unique=True),
        "customer_unique_id": Column(str, nullable=False),
        "customer_zip_code_prefix": Column(str, nullable=False,
                                          checks=Check.str_length(5)),
        "customer_city": Column(str, nullable=False),
        "customer_state": Column(str, nullable=False,
                                checks=Check.str_length(2)),
    },
    name="customers"
)


# Schéma pour olist_order_items_dataset
ORDER_ITEMS_SCHEMA = DataFrameSchema(
    {
        "order_id": Column(str, nullable=False),
        "order_item_id": Column(int, nullable=False, 
                               checks=Check.greater_than_or_equal_to(1)),
        "product_id": Column(str, nullable=False),
        "seller_id": Column(str, nullable=False),
        "shipping_limit_date": Column("datetime64[ns]", nullable=False),
        "price": Column(float, nullable=False,
                       checks=Check.greater_than_or_equal_to(0)),
        "freight_value": Column(float, nullable=False,
                               checks=Check.greater_than_or_equal_to(0)),
    },
    name="order_items"
)


# Schéma pour olist_order_payments_dataset
PAYMENTS_SCHEMA = DataFrameSchema(
    {
        "order_id": Column(str, nullable=False),
        "payment_sequential": Column(int, nullable=False,
                                    checks=Check.greater_than_or_equal_to(1)),
        "payment_type": Column(str, nullable=False,
                              checks=Check.isin(['credit_card', 'boleto', 'voucher', 
                                                'debit_card', 'not_defined'])),
        "payment_installments": Column(int, nullable=False,
                                      checks=Check.in_range(0, 24)),
        "payment_value": Column(float, nullable=False,
                               checks=Check.greater_than_or_equal_to(0)),
    },
    name="payments"
)


# Schéma pour olist_order_reviews_dataset
REVIEWS_SCHEMA = DataFrameSchema(
    {
        "review_id": Column(str, nullable=False, unique=True),
        "order_id": Column(str, nullable=False),
        "review_score": Column(int, nullable=False,
                              checks=Check.in_range(1, 5)),
        "review_comment_title": Column(str, nullable=True),
        "review_comment_message": Column(str, nullable=True),
        "review_creation_date": Column("datetime64[ns]", nullable=False),
        "review_answer_timestamp": Column("datetime64[ns]", nullable=False),
    },
    name="reviews"
)


# Schéma pour olist_products_dataset
PRODUCTS_SCHEMA = DataFrameSchema(
    {
        "product_id": Column(str, nullable=False, unique=True),
        "product_category_name": Column(str, nullable=True),
        "product_name_length": Column(float, nullable=True,
                                     checks=Check.greater_than(0)),
        "product_description_length": Column(float, nullable=True,
                                            checks=Check.greater_than(0)),
        "product_photos_qty": Column(float, nullable=True,
                                    checks=Check.greater_than_or_equal_to(0)),
        "product_weight_g": Column(float, nullable=True,
                                  checks=Check.greater_than(0)),
        "product_length_cm": Column(float, nullable=True,
                                   checks=Check.greater_than(0)),
        "product_height_cm": Column(float, nullable=True,
                                   checks=Check.greater_than(0)),
        "product_width_cm": Column(float, nullable=True,
                                  checks=Check.greater_than(0)),
    },
    name="products"
)


# Schéma pour olist_sellers_dataset
SELLERS_SCHEMA = DataFrameSchema(
    {
        "seller_id": Column(str, nullable=False, unique=True),
        "seller_zip_code_prefix": Column(str, nullable=False,
                                        checks=Check.str_length(5)),
        "seller_city": Column(str, nullable=False),
        "seller_state": Column(str, nullable=False,
                              checks=Check.str_length(2)),
    },
    name="sellers"
)


# Schéma pour olist_geolocation_dataset
GEOLOCATION_SCHEMA = DataFrameSchema(
    {
        "geolocation_zip_code_prefix": Column(str, nullable=False,
                                             checks=Check.str_length(5)),
        "geolocation_lat": Column(float, nullable=False,
                                 checks=Check.in_range(-90, 90)),
        "geolocation_lng": Column(float, nullable=False,
                                 checks=Check.in_range(-180, 180)),
        "geolocation_city": Column(str, nullable=False),
        "geolocation_state": Column(str, nullable=False,
                                   checks=Check.str_length(2)),
    },
    name="geolocation"
)


# Dictionnaire global de tous les schémas
OLIST_SCHEMAS = {
    'olist_orders_dataset': ORDERS_SCHEMA,
    'olist_customers_dataset': CUSTOMERS_SCHEMA,
    'olist_order_items_dataset': ORDER_ITEMS_SCHEMA,
    'olist_order_payments_dataset': PAYMENTS_SCHEMA,
    'olist_order_reviews_dataset': REVIEWS_SCHEMA,
    'olist_products_dataset': PRODUCTS_SCHEMA,
    'olist_sellers_dataset': SELLERS_SCHEMA,
    'olist_geolocation_dataset': GEOLOCATION_SCHEMA,
}


def get_schema(dataset_name: str) -> DataFrameSchema:
    """
    Récupère le schéma Pandera pour un dataset donné.
    
    Args:
        dataset_name: Nom du dataset (ex: 'olist_orders_dataset').
    
    Returns:
        DataFrameSchema correspondant.
    
    Raises:
        KeyError: Si le dataset n'a pas de schéma défini.
    """
    if dataset_name not in OLIST_SCHEMAS:
        available = ', '.join(OLIST_SCHEMAS.keys())
        raise KeyError(f"Schéma '{dataset_name}' introuvable. Disponibles: {available}")
    
    return OLIST_SCHEMAS[dataset_name]
