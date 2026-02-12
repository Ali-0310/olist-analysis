"""Script de téléchargement des données Olist depuis Kaggle."""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Vérifier que les credentials Kaggle sont configurés
if not os.getenv("KAGGLE_USERNAME") or not os.getenv("KAGGLE_KEY"):
    print("❌ ERREUR: Credentials Kaggle manquants!")
    print("\nConfigurez vos credentials dans le fichier .env:")
    print("1. Copiez .env.example vers .env")
    print("2. Remplissez KAGGLE_USERNAME et KAGGLE_KEY")
    print("   (obtenez-les depuis https://www.kaggle.com/settings)")
    sys.exit(1)

# Ajouter le dossier src au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.loader import OlistDataLoader


def main():
    """Télécharge et affiche un résumé du dataset Olist."""
    print("="*60)
    print("TÉLÉCHARGEMENT DU DATASET OLIST")
    print("="*60)
    
    # Initialiser le loader
    loader = OlistDataLoader()
    
    # Télécharger et charger toutes les données
    loader.load_all()
    
    # Afficher un résumé
    print("\n" + "="*60)
    print("RÉSUMÉ DES FICHIERS CHARGÉS")
    print("="*60)
    print(loader.summary().to_string(index=False))
    print("\n✓ Téléchargement terminé avec succès!")


if __name__ == "__main__":
    main()
