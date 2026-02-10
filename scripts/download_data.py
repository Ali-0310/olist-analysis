"""Script de téléchargement des données Olist depuis Kaggle."""

import sys
from pathlib import Path

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
