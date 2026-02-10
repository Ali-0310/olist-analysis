"""Définition du schéma SQL pour les données Olist."""

from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# NOTE: Ce fichier sera complété après l'analyse descriptive
# pour déduire le schéma optimal depuis les données réelles.

# Exemple de structure (à adapter selon analyse):
# class Order(Base):
#     __tablename__ = 'orders'
#     order_id = Column(String, primary_key=True)
#     customer_id = Column(String, ForeignKey('customers.customer_id'))
#     order_status = Column(String)
#     order_purchase_timestamp = Column(DateTime)
#     ...
