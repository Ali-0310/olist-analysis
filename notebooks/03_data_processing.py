"""Notebook Marimo - Processing et préparation des données pour Silver."""

import marimo

__generated_with = "0.8.22"
app = marimo.App()


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import numpy as np
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path.cwd().parent))

    from src.database.connection import DatabaseConnection
    from src.database.writer import DatabaseWriter
    from src.utils.config import config
    return (
        DatabaseConnection,
        DatabaseWriter,
        Path,
        config,
        go,
        mo,
        np,
        pd,
        px,
        sys,
    )


@app.cell
def __(mo):
    mo.md(
        """
        # 🔧 Data Processing - Préparation pour Silver Layer

        Ce notebook traite les données Bronze et les prépare pour la couche Silver.
        """
    )
    return


@app.cell
def __(mo):
    mo.md("""## 1️⃣ Chargement des données depuis Bronze Layer""")
    return


@app.cell
async def __(DatabaseConnection, config, pd):
    # Connexion à SQLite Bronze
    db_conn = DatabaseConnection(
        db_type=config.DB_TYPE,
        db_path=config.DB_PATH
    )

    # Charger toutes les tables Bronze
    bronze_tables = {}
    with db_conn:
        conn = db_conn.connection
        _cursor = conn.cursor()

        # Lister les tables Bronze
        _cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'bronze_%'")
        table_names = [row[0] for row in _cursor.fetchall()]

        # Charger chaque table
        for _table_name in table_names:
            clean_name = _table_name.replace('bronze_', '')
            bronze_tables[clean_name] = pd.read_sql_query(f"SELECT * FROM {_table_name}", conn)
    return bronze_tables, clean_name, conn, db_conn, table_names


@app.cell
def __(bronze_tables, mo):
    mo.md(f"""
    ✅ **{len(bronze_tables)} tables Bronze chargées**

    Tables : {', '.join(bronze_tables.keys())}
    """)
    return


@app.cell
def __(mo):
    mo.md("""## 2️⃣ Processing des Données par Table""")
    return


@app.cell
def __(mo):
    mo.md(
        """
        ### 📍 Table: olist_geolocation_dataset

        Validation des coordonnées géographiques et détection des doublons.
        """
    )
    return


@app.cell
def __(bronze_tables, mo):
    geo_df = bronze_tables['olist_geolocation_dataset'].copy()

    # Vérifier les coordonnées valides
    valid_lat = (geo_df['geolocation_lat'] >= -90) & (geo_df['geolocation_lat'] <= 90)
    valid_lng = (geo_df['geolocation_lng'] >= -180) & (geo_df['geolocation_lng'] <= 180)

    invalid_coords = geo_df[~(valid_lat & valid_lng)]

    # Afficher les résultats
    geo_result = mo.vstack([
        mo.md(f"""
        **Validation des coordonnées:</br>**
        - Total lignes: {len(geo_df):,}</br>
        - Coordonnées invalides: {len(invalid_coords):,}</br>
        - Latitude valide (-90 à +90): {valid_lat.sum():,} / {len(geo_df):,}</br>
        - Longitude valide (-180 à +180): {valid_lng.sum():,} / {len(geo_df):,}</br>
        """),
        mo.ui.table(invalid_coords.head(10)) if len(invalid_coords) > 0 else mo.md("✅ Toutes les coordonnées sont valides")
    ])

    geo_result
    return geo_df, geo_result, invalid_coords, valid_lat, valid_lng


@app.cell
def __(geo_df, mo):
    # Afficher les doublons
    geo_duplicates = geo_df[geo_df.duplicated(subset=["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng"], keep=False)]

    mo.vstack([
        mo.md(f"""
        **Doublons détectés:**
        - Lignes dupliquées: {len(geo_duplicates):,}
        - Pourcentage: {len(geo_duplicates)/len(geo_df)*100:.2f}%
        """),
        mo.ui.table(geo_duplicates) if len(geo_duplicates) > 0 else mo.md("✅ Aucun doublon")
    ])
    return (geo_duplicates,)


@app.cell
def __(geo_duplicates):
    geo_duplicates[(geo_duplicates["geolocation_zip_code_prefix"]==	96230)&(geo_duplicates["geolocation_lat"]==	-33.531439999999975)&(geo_duplicates["geolocation_lng"]==-53.35056999999995)]
    return


@app.cell
def __(mo):
    mo.md(r"""#### Suppression des doublons en prenant en compte les colonnes zip code + long +lat""")
    return


@app.cell
def __(geo_df):
    geo_df_cleaned = geo_df.drop_duplicates(subset=["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng"])
    return (geo_df_cleaned,)


@app.cell
def __(geo_df_cleaned):
    geo_df_cleaned[(geo_df_cleaned["geolocation_zip_code_prefix"]==	96230)&(geo_df_cleaned["geolocation_lat"]==	-33.531439999999975)&(geo_df_cleaned["geolocation_lng"]==-53.35056999999995)]
    return


@app.cell
def __(geo_df_cleaned, mo):
    # Afficher les doublons
    geo_duplicates_after_cleaning = geo_df_cleaned[geo_df_cleaned.duplicated(subset=["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng"], keep=False)]

    mo.vstack([
        mo.md(f"""
        **Doublons Après nettoyage:**
        - Lignes dupliquées: {len(geo_duplicates_after_cleaning):,}
        - Pourcentage: {len(geo_duplicates_after_cleaning)/len(geo_df_cleaned)*100:.2f}%
        """),
        mo.ui.table(geo_duplicates_after_cleaning) if len(geo_duplicates_after_cleaning) > 0 else mo.md("✅ Aucun doublon")
    ])
    return (geo_duplicates_after_cleaning,)


@app.cell
def __(mo):
    mo.md(
        """
        ### 📅 Table: olist_orders_dataset

        Conversion des colonnes de dates en datetime.
        """
    )
    return


@app.cell
async def __(bronze_tables, mo, pd):
    orders_df = bronze_tables['olist_orders_dataset'].copy()

    # Colonnes à convertir
    date_columns = [
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]

    # Conversion en datetime
    for col in date_columns:
        orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')

    # Afficher les résultats
    orders_result = mo.vstack([
        mo.md("""
        **Conversion des dates:**
        """),
        mo.ui.table(pd.DataFrame({
            'Colonne': date_columns,
            'Type avant': ['object'] * len(date_columns),
            'Type après': [str(orders_df[col].dtype) for col in date_columns],
            'Valeurs null': [orders_df[col].isnull().sum() for col in date_columns]
        })),
        mo.md("**Aperçu:**"),
        mo.ui.table(orders_df[date_columns].head(10))
    ])

    orders_result
    return col, date_columns, orders_df, orders_result


@app.cell
def __(mo):
    mo.md(
        """
        ### 📦 Table: olist_order_items_dataset

        Conversion date + outliers sur price et freight_value.
        """
    )
    return


@app.cell
def __(bronze_tables, mo, pd, px):
    items_df = bronze_tables['olist_order_items_dataset'].copy()

    # Conversion datetime
    items_df['shipping_limit_date'] = pd.to_datetime(items_df['shipping_limit_date'], errors='coerce')

    # Détection outliers pour price
    Q1_price = items_df['price'].quantile(0.25)
    Q3_price = items_df['price'].quantile(0.75)
    IQR_price = Q3_price - Q1_price
    price_outliers = items_df[(items_df['price'] < (Q1_price - 1.5 * IQR_price)) | 
                               (items_df['price'] > (Q3_price + 1.5 * IQR_price))]

    # Détection outliers pour freight_value
    Q1_freight = items_df['freight_value'].quantile(0.25)
    Q3_freight = items_df['freight_value'].quantile(0.75)
    IQR_freight = Q3_freight - Q1_freight
    freight_outliers = items_df[(items_df['freight_value'] < (Q1_freight - 1.5 * IQR_freight)) | 
                                 (items_df['freight_value'] > (Q3_freight + 1.5 * IQR_freight))]

    # Visualisation outliers price
    fig_price = px.box(items_df, y='price', title='Distribution Price (avec outliers)')

    # Visualisation outliers freight_value
    fig_freight = px.box(items_df, y='freight_value', title='Distribution Freight Value (avec outliers)')

    items_result = mo.vstack([
        mo.md(f"""
        **Conversion datetime:**
        - shipping_limit_date: {items_df['shipping_limit_date'].dtype}

        **Outliers détectés:**
        - Price: {len(price_outliers):,} outliers ({len(price_outliers)/len(items_df)*100:.2f}%)
        - Freight Value: {len(freight_outliers):,} outliers ({len(freight_outliers)/len(items_df)*100:.2f}%)
        """),
        mo.ui.plotly(fig_price),
        mo.md("**Outliers Price (Top 10):**"),
        mo.ui.table(price_outliers.nlargest(10, 'price')[['order_id', 'product_id', 'price']]),
        mo.ui.plotly(fig_freight),
        mo.md("**Outliers Freight Value (Top 10):**"),
        mo.ui.table(freight_outliers.nlargest(10, 'freight_value')[['order_id', 'product_id', 'freight_value']])
    ])

    items_result
    return (
        IQR_freight,
        IQR_price,
        Q1_freight,
        Q1_price,
        Q3_freight,
        Q3_price,
        fig_freight,
        fig_price,
        freight_outliers,
        items_df,
        items_result,
        price_outliers,
    )


@app.cell
def __(mo):
    mo.md(
        """
        ### 💳 Table: olist_order_payments_dataset

        Outliers sur payment_installments et payment_value.
        """
    )
    return


@app.cell
def __(bronze_tables, mo, px):
    payments_df = bronze_tables['olist_order_payments_dataset'].copy()

    # Outliers payment_installments
    Q1_inst = payments_df['payment_installments'].quantile(0.25)
    Q3_inst = payments_df['payment_installments'].quantile(0.75)
    IQR_inst = Q3_inst - Q1_inst
    installments_outliers = payments_df[(payments_df['payment_installments'] < (Q1_inst - 1.5 * IQR_inst)) | 
                                         (payments_df['payment_installments'] > (Q3_inst + 1.5 * IQR_inst))]

    # Outliers payment_value
    Q1_val = payments_df['payment_value'].quantile(0.25)
    Q3_val = payments_df['payment_value'].quantile(0.75)
    IQR_val = Q3_val - Q1_val
    value_outliers = payments_df[(payments_df['payment_value'] < (Q1_val - 1.5 * IQR_val)) | 
                                  (payments_df['payment_value'] > (Q3_val + 1.5 * IQR_val))]

    # Visualisations
    fig_inst = px.box(payments_df, y='payment_installments', title='Distribution Payment Installments')
    fig_val = px.box(payments_df, y='payment_value', title='Distribution Payment Value')

    payments_result = mo.vstack([
        mo.md(f"""
        **Outliers détectés:**
        - Payment Installments: {len(installments_outliers):,} outliers ({len(installments_outliers)/len(payments_df)*100:.2f}%)
        - Payment Value: {len(value_outliers):,} outliers ({len(value_outliers)/len(payments_df)*100:.2f}%)
        """),
        mo.ui.plotly(fig_inst),
        mo.md("**Outliers Payment Installments (Top 10):**"),
        mo.ui.table(installments_outliers.nlargest(10, 'payment_installments')[['order_id', 'payment_installments', 'payment_value']]),
        mo.ui.plotly(fig_val),
        mo.md("**Outliers Payment Value (Top 10):**"),
        mo.ui.table(value_outliers.nlargest(10, 'payment_value')[['order_id', 'payment_type', 'payment_value']])
    ])

    payments_result
    return (
        IQR_inst,
        IQR_val,
        Q1_inst,
        Q1_val,
        Q3_inst,
        Q3_val,
        fig_inst,
        fig_val,
        installments_outliers,
        payments_df,
        payments_result,
        value_outliers,
    )


@app.cell
def __(mo):
    mo.md(
        """
        ### ⭐ Table: olist_order_reviews_dataset

        Suppression colonnes commentaires, conversion dates, outliers review_score.
        """
    )
    return


@app.cell
def __(bronze_tables, mo, pd, px):
    reviews_df = bronze_tables['olist_order_reviews_dataset'].copy()

    # Supprimer les colonnes de commentaires
    columns_to_drop = ['review_comment_title', 'review_comment_message']
    reviews_df = reviews_df.drop(columns=columns_to_drop, errors='ignore')

    # Conversion datetime
    reviews_df['review_creation_date'] = pd.to_datetime(reviews_df['review_creation_date'], errors='coerce')
    reviews_df['review_answer_timestamp'] = pd.to_datetime(reviews_df['review_answer_timestamp'], errors='coerce')

    # Outliers review_score (notes hors de 1-5)
    score_outliers = reviews_df[(reviews_df['review_score'] < 1) | (reviews_df['review_score'] > 5)]

    # Distribution des scores
    fig_score = px.histogram(reviews_df, x='review_score', 
                              title='Distribution Review Score',
                              nbins=5)

    reviews_result = mo.vstack([
        mo.md(f"""
        **Modifications:**</br>
        - Colonnes supprimées: {', '.join(columns_to_drop)}</br>
        - Dates converties: review_creation_date ({reviews_df['review_creation_date'].dtype}), review_answer_timestamp ({reviews_df['review_answer_timestamp'].dtype})</br>
        - Outliers review_score (hors 1-5): {len(score_outliers):,}</br>
        """),
        mo.ui.plotly(fig_score),
        mo.md("**Aperçu des données nettoyées:**"),
        mo.ui.table(reviews_df.head(10))
    ])

    reviews_result
    return (
        columns_to_drop,
        fig_score,
        reviews_df,
        reviews_result,
        score_outliers,
    )


@app.cell
def __(mo):
    mo.md(
        """
        ### 🛍️ Table: olist_products_dataset

        Outliers + fusion avec product_category_name_translation.
        """
    )
    return


@app.cell
def __(bronze_tables, mo, px):
    products_df = bronze_tables['olist_products_dataset'].copy()
    translation_df = bronze_tables['product_category_name_translation'].copy()

    # Outliers pour product_description_lenght
    Q1_desc = products_df['product_description_lenght'].quantile(0.25)
    Q3_desc = products_df['product_description_lenght'].quantile(0.75)
    IQR_desc = Q3_desc - Q1_desc
    desc_outliers = products_df[(products_df['product_description_lenght'] < (Q1_desc - 1.5 * IQR_desc)) | 
                                 (products_df['product_description_lenght'] > (Q3_desc + 1.5 * IQR_desc))]

    # Outliers pour product_weight_g
    Q1_weight = products_df['product_weight_g'].quantile(0.25)
    Q3_weight = products_df['product_weight_g'].quantile(0.75)
    IQR_weight = Q3_weight - Q1_weight
    weight_outliers = products_df[(products_df['product_weight_g'] < (Q1_weight - 1.5 * IQR_weight)) | 
                                   (products_df['product_weight_g'] > (Q3_weight + 1.5 * IQR_weight))]

    # Outliers pour product_height_cm
    Q1_height = products_df['product_height_cm'].quantile(0.25)
    Q3_height = products_df['product_height_cm'].quantile(0.75)
    IQR_height = Q3_height - Q1_height
    height_outliers = products_df[(products_df['product_height_cm'] < (Q1_height - 1.5 * IQR_height)) | 
                                   (products_df['product_height_cm'] > (Q3_height + 1.5 * IQR_height))]

    # Fusion avec translation
    products_merged = products_df.merge(
        translation_df[['product_category_name', 'product_category_name_english']],
        on='product_category_name',
        how='left'
    )

    # Remplacer product_category_name par la traduction
    products_merged['product_category_name'] = products_merged['product_category_name_english'].fillna(products_merged['product_category_name'])
    products_merged = products_merged.drop('product_category_name_english', axis=1)

    # Visualisations
    fig_desc = px.box(products_df, y='product_description_lenght', title='Distribution Description Length')
    fig_weight = px.box(products_df, y='product_weight_g', title='Distribution Weight (g)')
    fig_height = px.box(products_df, y='product_height_cm', title='Distribution Height (cm)')

    products_result = mo.vstack([
        mo.md(f"""
        **Outliers détectés:**
        - Description Length: {len(desc_outliers):,} outliers ({len(desc_outliers)/len(products_df)*100:.2f}%)
        - Weight (g): {len(weight_outliers):,} outliers ({len(weight_outliers)/len(products_df)*100:.2f}%)
        - Height (cm): {len(height_outliers):,} outliers ({len(height_outliers)/len(products_df)*100:.2f}%)

        **Fusion avec traduction:**
        - Catégories traduites: {products_merged['product_category_name'].notna().sum():,}
        """),
        mo.ui.plotly(fig_desc),
        mo.ui.plotly(fig_weight),
        mo.ui.plotly(fig_height),
        mo.md("**Aperçu des produits avec traduction:**"),
        mo.ui.table(products_merged[['product_id', 'product_category_name', 'product_weight_g']].head(10))
    ])

    products_result
    return (
        IQR_desc,
        IQR_height,
        IQR_weight,
        Q1_desc,
        Q1_height,
        Q1_weight,
        Q3_desc,
        Q3_height,
        Q3_weight,
        desc_outliers,
        fig_desc,
        fig_height,
        fig_weight,
        height_outliers,
        products_df,
        products_merged,
        products_result,
        translation_df,
        weight_outliers,
    )


@app.cell
def __(products_merged):
    products_merged["product_category_name"].value_counts().head(3).to_frame()
    return


@app.cell
def __(mo):
    mo.md(
        """
        ## 3️⃣ Chargement dans Silver Layer

        Les tables traitées sont prêtes à être chargées dans la couche Silver avec indexes optimisés.
        """
    )
    return


@app.cell
def __(mo):
    load_silver_button = mo.ui.run_button(
        label="🥈 Charger dans Silver avec Indexes",
        kind="success"
    )
    load_silver_button
    return (load_silver_button,)


@app.cell
async def __(
    DatabaseConnection,
    DatabaseWriter,
    bronze_tables,
    config,
    geo_df_cleaned,
    items_df,
    load_silver_button,
    mo,
    orders_df,
    payments_df,
    products_merged,
    reviews_df,
):
    if load_silver_button.value:
        # Préparer les DataFrames nettoyés
        cleaned_tables = {
            'olist_geolocation_dataset': geo_df_cleaned,
            'olist_orders_dataset': orders_df,
            'olist_order_items_dataset': items_df,
            'olist_order_payments_dataset': payments_df,
            'olist_order_reviews_dataset': reviews_df,
            'olist_products_dataset': products_merged,
        }

        # Ajouter les autres tables non modifiées
        for table_name, table_df in bronze_tables.items():
            if table_name not in cleaned_tables:
                cleaned_tables[table_name] = table_df

        # Connexion et écriture
        db_connection = DatabaseConnection(
            db_type=config.DB_TYPE,
            db_path=config.DB_PATH
        )

        with db_connection:
            writer = DatabaseWriter(db_connection)

            # Écrire dans Silver
            writer.write_multiple(cleaned_tables, schema='silver', if_exists='replace')

            # Créer les indexes
            _cursor = db_connection.connection.cursor()

            # Index pour olist_orders_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_order_id ON silver_olist_orders_dataset(order_id)")
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_customer_id ON silver_olist_orders_dataset(customer_id)")
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_delivered_date ON silver_olist_orders_dataset(order_delivered_customer_date)")
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_orders_estimated_date ON silver_olist_orders_dataset(order_estimated_delivery_date)")

            # Index pour olist_order_items_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_items_order_id ON silver_olist_order_items_dataset(order_id)")
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_items_product_id ON silver_olist_order_items_dataset(product_id)")
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_items_seller_id ON silver_olist_order_items_dataset(seller_id)")

            # Index pour olist_customers_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_customers_customer_id ON silver_olist_customers_dataset(customer_id)")

            # Index pour olist_products_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_products_product_id ON silver_olist_products_dataset(product_id)")

            # Index pour olist_sellers_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_sellers_seller_id ON silver_olist_sellers_dataset(seller_id)")

            # Index pour olist_order_payments_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_payments_order_id ON silver_olist_order_payments_dataset(order_id)")

            # Index pour olist_order_reviews_dataset
            _cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_reviews_order_id ON silver_olist_order_reviews_dataset(order_id)")

            db_connection.connection.commit()

        silver_result = mo.md(f"""
        ### ✅ Chargement Silver terminé !

        - **Base de données**: `{config.DB_PATH}`
        - **Tables créées**: {len(cleaned_tables)} tables dans Silver
        - **Indexes créés**: 12 indexes pour optimisation des requêtes
        - **Total lignes**: {sum(len(df) for df in cleaned_tables.values()):,}

        **Prochaine étape** : Créer les métriques business → Gold layer (`create_gold_metrics.py`)
        """)
    else:
        silver_result = mo.md("_Cliquez sur le bouton ci-dessus pour charger dans Silver_")

    silver_result
    return (
        cleaned_tables,
        db_connection,
        silver_result,
        table_df,
        table_name,
        writer,
    )


if __name__ == "__main__":
    app.run()
