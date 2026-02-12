"""Notebook Marimo - Optimisation SQL et comparaison Bronze/Silver."""

import marimo

__generated_with = "0.8.22"
app = marimo.App(layout_file="layouts/04_sql_optimization.slides.json")


@app.cell
def __():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path
    from sqlalchemy import create_engine, text, inspect
    import time

    sys.path.insert(0, str(Path.cwd().parent))

    from src.utils.config import config

    mo.md("# 🚀 Optimisation SQL - Architecture Medallion")
    return Path, config, create_engine, inspect, mo, pd, sys, text, time


@app.cell
def __(config, create_engine, mo):
    """Connexion à la base SQLite."""
    # Connexion unique réutilisable
    engine = create_engine(f"sqlite:///{config.DB_PATH}")
    connection = engine.connect()

    mo.vstack([
        mo.md("""
        # 📊 Connexion à la base de données
        Base SQLite avec architecture **Medallion** (Bronze/Silver/Gold)
        """),
        mo.md(f"✅ Connecté à : `{config.DB_PATH}`")
    ])
    return connection, engine


@app.cell
async def __(connection, inspect, mo, pd):
    """Liste des tables Bronze et Silver."""
    inspector = inspect(connection)
    all_tables = inspector.get_table_names()

    bronze_tables = [t for t in all_tables if t.startswith('bronze_')]
    silver_tables = [t for t in all_tables if t.startswith('silver_')]

    tables_summary = pd.DataFrame({
        'Schéma': ['Bronze', 'Silver'],
        'Nombre de tables': [len(bronze_tables), len(silver_tables)],
        'Préfixe': ['bronze_*', 'silver_*']
    })

    mo.vstack([
        mo.md("## 📋 Tables disponibles"),
        mo.ui.table(tables_summary)
    ])
    return (
        all_tables,
        bronze_tables,
        inspector,
        silver_tables,
        tables_summary,
    )


@app.cell
def __(bronze_tables, mo, silver_tables):
    """Affichage détaillé des tables."""
    mo.md(f"""
    ### 🥉 Tables Bronze ({len(bronze_tables)})
    `{', '.join(bronze_tables)}`

    ### 🥈 Tables Silver ({len(silver_tables)})  
    `{', '.join(silver_tables)}`
    """)
    return


@app.cell
def __(mo):
    """Éditeur SQL interactif."""

    # Éditeur SQL
    sql_editor = mo.ui.text_area(
        value="SELECT name FROM sqlite_master WHERE type='table';",
        label="Requête SQL",
        full_width=True,
        rows=8
    )

    # Bouton d'exécution
    execute_button = mo.ui.run_button(label="▶️ Exécuter")

    mo.vstack([
        mo.md("""
        ## ⚡ Éditeur SQL Interactif

        Testez vos requêtes SQL sur les tables Bronze et Silver
        """),
        mo.hstack([sql_editor, execute_button], justify="start")
    ])
    return execute_button, sql_editor


@app.cell
def __(connection, execute_button, mo, pd, sql_editor, time):
    """Exécution de la requête SQL."""
    result_display = None

    if execute_button.value and sql_editor.value.strip():
        try:
            start_time = time.time()
            result_df = pd.read_sql_query(sql_editor.value, connection)
            execution_time = (time.time() - start_time) * 1000  # en ms

            result_display = mo.vstack([
                mo.md(f"""
                ### ✅ Résultats ({len(result_df)} lignes)
                ⏱️ Temps d'exécution: **{execution_time:.2f} ms**
                """),
                mo.ui.table(result_df, page_size=20)
            ])

        except Exception as e:
            result_display = mo.md(f"""
            ### ❌ Erreur SQL
            ```
            {str(e)}
            ```
            """)

    result_display
    return execution_time, result_df, result_display, start_time


@app.cell
def __(mo):
    mo.md("""# Partie 1 : Exercice Window Function + CTE""")
    return


@app.cell
def __(mo):
    """Question 1 : CTE + RANK()"""
    mo.md("""
    ## 🎯 Question 1 : Classement des clients par montant total

    **Objectif** : Récupérer le classement de chaque client en fonction du montant total de ses paiements.

    **Techniques utilisées** :
    - 🔹 **CTE (Common Table Expression)** : Pour agréger les paiements par client
    - 🔹 **Window Function RANK()** : Pour classer les clients par montant décroissant
    """)
    return


@app.cell
def __(mo):
    """Éditeur SQL Question 1"""

    query_1 = """
    WITH customer_payments AS (
    -- CTE : Agréger les paiements par customer_id
    SELECT 
        o.customer_id,
        SUM(p.payment_value) AS total_payment
    FROM bronze_olist_order_payments_dataset p
    JOIN bronze_olist_orders_dataset o 
        ON p.order_id = o.order_id
    GROUP BY o.customer_id
    )
    SELECT 
        customer_id,
        ROUND(total_payment, 2) AS total_payment,
        RANK() OVER (ORDER BY total_payment DESC) AS customer_rank
    FROM customer_payments
    ORDER BY customer_rank
    LIMIT 20;
    """

    sql_editor_q1 = mo.ui.text_area(
        value=query_1,
        label="📝 Requête SQL - Question 1",
        full_width=True,
        rows=15
    )

    execute_button_q1 = mo.ui.run_button(label="▶️ Exécuter Question 1")

    mo.hstack([sql_editor_q1, execute_button_q1], justify="start")
    return execute_button_q1, query_1, sql_editor_q1


@app.cell
def __(connection, execute_button_q1, mo, pd, sql_editor_q1, time):
    """Exécution Question 1"""
    result_display_q1 = None

    if execute_button_q1.value and sql_editor_q1.value.strip():
        try:
            start_time_q1 = time.time()
            result_df_q1 = pd.read_sql_query(sql_editor_q1.value, connection)
            execution_time_q1 = (time.time() - start_time_q1) * 1000

            result_display_q1 = mo.vstack([
                mo.md(f"""
                ### ✅ Top 20 clients par montant total
                ⏱️ Temps d'exécution: **{execution_time_q1/1000:.2f} s** | 📊 **{len(result_df_q1)} résultats**
                """),
                mo.ui.table(result_df_q1, page_size=20)
            ])

        except Exception as e:
            result_display_q1 = mo.md(f"""
            ### ❌ Erreur SQL - Question 1
            ```
            {str(e)}
            ```
            """)

    result_display_q1
    return execution_time_q1, result_df_q1, result_display_q1, start_time_q1


@app.cell
def __(mo):
    """Explications Question 1"""
    mo.md("""
    ### 📚 Explications techniques

    #### 1️⃣ CTE (Common Table Expression)
    ```sql
    WITH customer_payments AS (...)
    ```
    - Crée une table temporaire `customer_payments`
    - Agrège `SUM(payment_value)` par `customer_id`
    - Nécessite une jointure entre `payments` et `orders`

    #### 2️⃣ Window Function RANK()
    ```sql
    RANK() OVER (ORDER BY total_payment DESC)
    ```
    - Classe les clients du plus gros au plus petit montant
    - `RANK()` peut avoir des égalités (ex: rang 1, 1, 3)
    - Alternative : `DENSE_RANK()` (pas de saut : 1, 1, 2) ou `ROW_NUMBER()` (unique : 1, 2, 3)
    """)
    return


@app.cell
def __(mo):
    """Question 2 : AVG() OVER() avec PARTITION BY"""
    mo.md("""
    ---

    ## 🎯 Question 2 : Montant de paiement vs Moyenne client

    **Objectif** : Pour chaque commande, afficher le montant du paiement et le montant moyen des commandes du client.

    **Techniques utilisées** :
    - 🔹 **Window Function AVG() OVER()** : Calcul de moyenne sans GROUP BY
    - 🔹 **PARTITION BY** : Segmentation par client pour calculer sa moyenne personnelle
    """)
    return


@app.cell
def __(mo):
    """Éditeur SQL Question 2"""

    query_2 = """
    SELECT 
        o.order_id,
        o.customer_id,
        ROUND(p.payment_value, 2) AS payment_value,
        ROUND(
            AVG(p.payment_value) OVER (PARTITION BY o.customer_id),
            2
        ) AS avg_customer_payment,
        ROUND(
            p.payment_value - AVG(p.payment_value) OVER (PARTITION BY o.customer_id),
            2
        ) AS diff_from_avg
    FROM bronze_olist_orders_dataset o
    JOIN bronze_olist_order_payments_dataset p 
        ON o.order_id = p.order_id
    ORDER BY o.customer_id, o.order_id
    LIMIT 50;
    """

    sql_editor_q2 = mo.ui.text_area(
        value=query_2,
        label="📝 Requête SQL - Question 2",
        full_width=True,
        rows=15
    )

    execute_button_q2 = mo.ui.run_button(label="▶️ Exécuter Question 2")

    mo.hstack([sql_editor_q2, execute_button_q2], justify="start")
    return execute_button_q2, query_2, sql_editor_q2


@app.cell
def __(connection, execute_button_q2, mo, pd, sql_editor_q2, time):
    """Exécution Question 2"""
    result_display_q2 = None

    if execute_button_q2.value and sql_editor_q2.value.strip():
        try:
            start_time_q2 = time.time()
            result_df_q2 = pd.read_sql_query(sql_editor_q2.value, connection)
            execution_time_q2 = (time.time() - start_time_q2) * 1000

            result_display_q2 = mo.vstack([
                mo.md(f"""
                ### ✅ Paiements avec moyenne par client
                ⏱️ Temps d'exécution: **{execution_time_q2/1000:.2f} s** | 📊 **{len(result_df_q2)} résultats**
                """),
                mo.ui.table(result_df_q2, page_size=20)
            ])

        except Exception as e:
            result_display_q2 = mo.md(f"""
            ### ❌ Erreur SQL - Question 2
            ```
            {str(e)}
            ```
            """)

    result_display_q2
    return execution_time_q2, result_df_q2, result_display_q2, start_time_q2


@app.cell
def __(mo):
    """Explications Question 2"""
    mo.md("""
    ### 📚 Explications techniques

    #### 1️⃣ Window Function AVG() OVER()
    ```sql
    AVG(payment_value) OVER (PARTITION BY customer_id)
    ```
    - **Différence avec GROUP BY** : Conserve toutes les lignes (pas d'agrégation)
    - Calcule la moyenne **par fenêtre** (ici par client)
    - Chaque ligne garde ses détails + la moyenne de son groupe

    #### 2️⃣ PARTITION BY
    - Divise les données en **partitions** (segments)
    - Ici : une partition = un client
    - La fonction AVG() s'applique **indépendamment** dans chaque partition

    #### 3️⃣ Colonne bonus : diff_from_avg
    - Calcule `payment_value - avg_customer_payment`
    - Permet d'identifier les commandes **au-dessus** (>0) ou **en-dessous** (<0) de la moyenne du client
    """)
    return


@app.cell
def __(mo):
    """Question 3 : LAG() pour commandes consécutives"""
    mo.md("""
    ---

    ## 🎯 Question 3 : Délai entre ventes consécutives par vendeur

    **Objectif** : Calculer la différence en jours entre deux ventes consécutives d'un même vendeur.

    **Techniques utilisées** :
    - 🔹 **CTE (Common Table Expression)** : Filtrer les vendeurs avec plusieurs ventes
    - 🔹 **Window Function LAG()** : Accéder à la ligne précédente dans la fenêtre
    - 🔹 **PARTITION BY + ORDER BY** : Segmentation par vendeur + tri chronologique
    - 🔹 **Calcul de dates** : Différence en jours avec julianday() (SQLite)
    """)
    return


@app.cell
def __(mo):
    """Éditeur SQL Question 3"""

    query_3 = """
    WITH sellers_with_multiple_sales AS (
        -- CTE : Sélectionner les vendeurs avec plusieurs ventes
        SELECT seller_id
        FROM bronze_olist_order_items_dataset
        GROUP BY seller_id
        HAVING COUNT(*) >= 2
    ),
    seller_orders AS (
        -- CTE : Récupérer les commandes des vendeurs actifs
        SELECT 
            oi.seller_id,
            oi.order_id,
            o.order_purchase_timestamp
        FROM bronze_olist_order_items_dataset oi
        JOIN bronze_olist_orders_dataset o 
            ON oi.order_id = o.order_id
        WHERE oi.seller_id IN (SELECT seller_id FROM sellers_with_multiple_sales)
    )
    SELECT 
        seller_id,
        order_purchase_timestamp,
        LAG(order_purchase_timestamp) OVER (
            PARTITION BY seller_id 
            ORDER BY order_purchase_timestamp 
        ) AS previous_sale_timestamp,
        ROUND(
            (julianday(order_purchase_timestamp) - 
             julianday(LAG(order_purchase_timestamp) OVER (
                PARTITION BY seller_id 
                ORDER BY order_purchase_timestamp 
             ))
            ),
            1
        ) AS days_since_previous_sale
    FROM seller_orders
    ORDER BY days_since_previous_sale DESC
    LIMIT 50;
    """

    sql_editor_q3 = mo.ui.text_area(
        value=query_3,
        label="📝 Requête SQL - Question 3",
        full_width=True,
        rows=25
    )

    execute_button_q3 = mo.ui.run_button(label="▶️ Exécuter Question 3")

    mo.hstack([sql_editor_q3, execute_button_q3], justify="start")
    return execute_button_q3, query_3, sql_editor_q3


@app.cell
def __(connection, execute_button_q3, mo, pd, sql_editor_q3, time):
    """Exécution Question 3"""
    result_display_q3 = None

    if execute_button_q3.value and sql_editor_q3.value.strip():
        try:
            start_time_q3 = time.time()
            result_df_q3 = pd.read_sql_query(sql_editor_q3.value, connection)
            execution_time_q3 = (time.time() - start_time_q3)

            result_display_q3 = mo.vstack([
                mo.md(f"""
                ### ✅ Délai entre ventes consécutives par vendeur
                ⏱️ Temps d'exécution: **{execution_time_q3:.2f}s** | 📊 **{len(result_df_q3)} résultats**
                """),
                mo.ui.table(result_df_q3, page_size=20)
            ])

        except Exception as e:
            result_display_q3 = mo.md(f"""
            ### ❌ Erreur SQL - Question 3
            ```
            {str(e)}
            ```
            """)

    result_display_q3
    return execution_time_q3, result_df_q3, result_display_q3, start_time_q3


@app.cell
def __(mo):
    """Explications Question 3"""
    mo.md("""
    ### 📚 Explications techniques

    #### 1️⃣ CTEs multiples (Common Table Expressions)
    ```sql
    WITH sellers_with_multiple_sales AS (...),
         seller_orders AS (...)
    ```
    - **CTE 1** : Identifie les vendeurs avec `COUNT(*) >= 2` ventes
    - **CTE 2** : Récupère les commandes + timestamps pour ces vendeurs
    - Les CTEs peuvent être chaînées et réutilisées entre elles

    #### 2️⃣ Window Function LAG()
    ```sql
    LAG(order_purchase_timestamp) OVER (
        PARTITION BY seller_id 
        ORDER BY order_purchase_timestamp
    )
    ```
    - **LAG()** : Accède à la valeur de la **ligne précédente** dans la fenêtre
    - Retourne `NULL` pour la première vente de chaque vendeur
    - Alternative : **LEAD()** pour accéder à la ligne suivante

    #### 3️⃣ PARTITION BY + ORDER BY
    - **PARTITION BY seller_id** : Une fenêtre par vendeur
    - **ORDER BY order_purchase_timestamp** : Tri chronologique **dans chaque partition**
    - Garantit que LAG() récupère bien la vente **précédente du même vendeur**

    #### 4️⃣ Calcul de différence de dates (SQLite)
    ```sql
    julianday(date1) - julianday(date2)
    ```
    - **julianday()** : Convertit une date en nombre de jours depuis 4713 BC
    - La différence donne le nombre de jours entre deux dates
    - Alternative PostgreSQL : `date1 - date2` directement

    #### 5️⃣ Pourquoi vendeurs au lieu de clients ?
    - Dans le dataset Olist, chaque client a généralement **une seule commande**
    - Les **vendeurs (sellers)** ont plusieurs ventes, parfait pour démontrer LAG()
    - Utilise la table `order_items` qui contient `seller_id`
    """)
    return


@app.cell
def __(mo):
    """Étape 3 : Comparaison Bronze vs Silver avec EXPLAIN"""
    mo.md("""
    # Partie 2 : Impact de l'indexation (Bronze vs Silver)

    **Objectif** : Comparer les plans d'exécution entre Bronze (sans index) et Silver (avec 12 index).

    **Technique** : `EXPLAIN QUERY PLAN` pour analyser comment SQLite exécute les requêtes.

    **Indexes présents dans Silver** :
    - 📌 `silver_olist_orders_dataset` : order_id, customer_id, delivered_date, estimated_date
    - 📌 `silver_olist_order_items_dataset` : order_id, product_id, seller_id
    - 📌 `silver_olist_order_payments_dataset` : order_id
    - 📌 Et 5 autres tables...
    """)
    return


@app.cell
def __(mo):
    """Requête de test pour comparaison"""

    # Requête identique pour Bronze et Silver
    test_query = """
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_status,
        p.payment_value,
        i.product_id,
        i.seller_id
    FROM {table_orders} o
    JOIN {table_payments} p ON o.order_id = p.order_id
    JOIN {table_items} i ON o.order_id = i.order_id
    WHERE o.order_status = 'delivered'
    LIMIT 100;
    """

    query_bronze = test_query.format(
        table_orders='bronze_olist_orders_dataset',
        table_payments='bronze_olist_order_payments_dataset',
        table_items='bronze_olist_order_items_dataset'
    )

    query_silver = test_query.format(
        table_orders='silver_olist_orders_dataset',
        table_payments='silver_olist_order_payments_dataset',
        table_items='silver_olist_order_items_dataset'
    )

    mo.md(f"""
    ### 📝 Requête de test

    Jointure de 3 tables : `orders` ↔ `payments` ↔ `items`

    **Filtre** : `order_status = 'delivered'`
    """)
    return query_bronze, query_silver, test_query


@app.cell
def __(mo, query_bronze):
    """EXPLAIN pour Bronze"""

    explain_bronze_query = f"EXPLAIN QUERY PLAN {query_bronze}"

    sql_editor_bronze = mo.ui.text_area(
        value=explain_bronze_query,
        label="🥉 EXPLAIN Bronze (sans index)",
        full_width=True,
        rows=10
    )

    execute_explain_bronze = mo.ui.run_button(label="▶️ Analyser Bronze")

    mo.vstack([
        mo.md("### 🥉 Bronze : Sans indexation"),
        mo.hstack([sql_editor_bronze, execute_explain_bronze], justify="start")
    ])
    return execute_explain_bronze, explain_bronze_query, sql_editor_bronze


@app.cell
def __(
    connection,
    execute_explain_bronze,
    mo,
    pd,
    sql_editor_bronze,
    time,
):
    """Exécution EXPLAIN Bronze"""
    result_explain_bronze = None

    if execute_explain_bronze.value and sql_editor_bronze.value.strip():
        try:
            start_bronze = time.time()
            df_explain_bronze = pd.read_sql_query(sql_editor_bronze.value, connection)
            time_bronze = (time.time() - start_bronze) * 1000

            result_explain_bronze = mo.vstack([
                mo.md(f"**⏱️ Temps d'analyse : {time_bronze:.2f} ms**"),
                mo.ui.table(df_explain_bronze, page_size=20)
            ])
        except Exception as e:
            result_explain_bronze = mo.md(f"❌ Erreur : {str(e)}")

    result_explain_bronze
    return (
        df_explain_bronze,
        result_explain_bronze,
        start_bronze,
        time_bronze,
    )


@app.cell
def __(mo, query_silver):
    """EXPLAIN pour Silver"""

    explain_silver_query = f"EXPLAIN QUERY PLAN {query_silver}"

    sql_editor_silver = mo.ui.text_area(
        value=explain_silver_query,
        label="🥈 EXPLAIN Silver (avec index)",
        full_width=True,
        rows=10
    )

    execute_explain_silver = mo.ui.run_button(label="▶️ Analyser Silver")

    mo.vstack([
        mo.md("### 🥈 Silver : Avec indexation"),
        mo.hstack([sql_editor_silver, execute_explain_silver], justify="start")
    ])
    return execute_explain_silver, explain_silver_query, sql_editor_silver


@app.cell
def __(
    connection,
    execute_explain_silver,
    mo,
    pd,
    sql_editor_silver,
    time,
):
    """Exécution EXPLAIN Silver"""
    result_explain_silver = None

    if execute_explain_silver.value and sql_editor_silver.value.strip():
        try:
            start_silver = time.time()
            df_explain_silver = pd.read_sql_query(sql_editor_silver.value, connection)
            time_silver = (time.time() - start_silver) * 1000

            result_explain_silver = mo.vstack([
                mo.md(f"**⏱️ Temps d'analyse : {time_silver:.2f} ms**"),
                mo.ui.table(df_explain_silver, page_size=20)
            ])
        except Exception as e:
            result_explain_silver = mo.md(f"❌ Erreur : {str(e)}")

    result_explain_silver
    return (
        df_explain_silver,
        result_explain_silver,
        start_silver,
        time_silver,
    )


@app.cell
def __(mo):
    """Explications EXPLAIN QUERY PLAN"""
    mo.md("""
    ### 📚 Lecture du Query Plan (SQLite)

    #### Colonnes du résultat EXPLAIN
    - **id** : Identifiant de l'étape
    - **parent** : Étape parente (pour les sous-requêtes)
    - **notused** : Non utilisé
    - **detail** : Description de l'opération

    #### Termes clés dans `detail`

    ✅ **SEARCH ... USING INDEX** : Utilise un index permanent
    - `SEARCH TABLE ... USING INDEX idx_name` → **Index permanent utilisé !**
    - Recherche rapide via B-tree pré-construit

    ⚠️ **SEARCH ... USING AUTOMATIC COVERING INDEX** : Index temporaire
    - SQLite crée un index **à la volée** pour la jointure
    - **Coût caché** : Construction de l'index à chaque requête
    - Plus lent qu'un index permanent

    ❌ **SCAN TABLE** : Parcours séquentiel complet
    - `SCAN TABLE ...` → Lit toutes les lignes une par une
    - Le plus lent (mais nécessaire pour la table de départ)

    #### Résultats observés

    🥉 **Bronze** :
    ```
    SCAN TABLE bronze_olist_orders_dataset
    SEARCH ... USING AUTOMATIC COVERING INDEX (order_id=?)
    SEARCH ... USING AUTOMATIC COVERING INDEX (order_id=?)
    ```
    → Index temporaires créés **à chaque exécution**

    🥈 **Silver** :
    ```
    SCAN TABLE silver_olist_orders_dataset
    SEARCH ... USING INDEX idx_silver_payments_order_id
    SEARCH ... USING INDEX idx_silver_items_order_id
    ```
    → Index permanents **déjà en mémoire**

    #### Pourquoi Silver est meilleur ?

    1. **Index permanent vs temporaire** :
       - Bronze : Crée l'index → Utilise → Détruit (à chaque requête)
       - Silver : Index déjà présent (créé une seule fois)

    2. **Optimiseur de requêtes** :
       - Avec index permanents, SQLite peut mieux optimiser le plan d'exécution
       - Statistiques d'index disponibles pour choisir le meilleur plan

    3. **Performances répétées** :
       - 1 requête : Différence faible
       - 1000 requêtes : Bronze reconstruit l'index 1000 fois !
    """)
    return


@app.cell
def __(mo):
    """Comparaison temps réel Bronze vs Silver"""
    mo.md("""
    ---

    ### ⏱️ Comparaison de performances réelles

    Comparons le temps d'exécution **réel** (pas juste EXPLAIN) sur la même requête.
    """)
    return


@app.cell
def __(mo, query_bronze, query_silver):
    """Boutons d'exécution pour mesure de temps"""

    sql_timing_bronze = mo.ui.text_area(
        value=query_bronze,
        label="🥉 Requête Bronze",
        full_width=True,
        rows=8
    )

    sql_timing_silver = mo.ui.text_area(
        value=query_silver,
        label="🥈 Requête Silver",
        full_width=True,
        rows=8
    )

    execute_timing = mo.ui.run_button(label="⏱️ Comparer les performances")

    mo.vstack([
        mo.hstack([sql_timing_bronze, sql_timing_silver], widths=[1, 1]),
        execute_timing
    ])
    return execute_timing, sql_timing_bronze, sql_timing_silver


@app.cell
def __(
    connection,
    execute_timing,
    mo,
    pd,
    sql_timing_bronze,
    sql_timing_silver,
    time,
):
    """Exécution et comparaison de temps"""
    timing_result = None

    if execute_timing.value:
        try:
            # Exécution Bronze
            start_b = time.time()
            df_bronze_timing = pd.read_sql_query(sql_timing_bronze.value, connection)
            time_bronze_exec = (time.time() - start_b) * 1000

            # Exécution Silver
            start_s = time.time()
            df_silver_timing = pd.read_sql_query(sql_timing_silver.value, connection)
            time_silver_exec = (time.time() - start_s) * 1000

            # Calcul du gain
            gain_percent = ((time_bronze_exec - time_silver_exec) / time_bronze_exec) * 100 if time_bronze_exec > 0 else 0
            speedup = time_bronze_exec / time_silver_exec if time_silver_exec > 0 else 1

            timing_result = mo.vstack([
                mo.md(f"""
                ### 📊 Résultats de performance

                | Couche | Temps d'exécution | Méthode d'indexation |
                |--------|------------------|---------------------|
                | 🥉 **Bronze** | **{time_bronze_exec:.2f} ms** | AUTOMATIC COVERING INDEX (temporaire) |
                | 🥈 **Silver** | **{time_silver_exec:.2f} ms** | INDEX permanent |

                **🚀 Gain : {gain_percent:.1f}%** (Silver est **{speedup:.2f}x plus rapide**)

                ---

                #### 💡 Analyse

                {"✅ **Silver est plus performant**" if time_silver_exec < time_bronze_exec else "⚠️ Différence marginale sur ce jeu de données"}

                **Pourquoi Silver est plus efficace ?**
                - Index permanents pré-construits et optimisés
                - Pas de création d'index temporaire à chaque requête  
                - Statistiques d'index pour meilleur plan d'exécution

                **Sur un volume important (millions de lignes)** :
                - Le gain serait beaucoup plus significatif
                - Les index temporaires coûtent de plus en plus cher à construire
                """),
                mo.md(f"**Lignes retournées** : {len(df_bronze_timing)} (Bronze), {len(df_silver_timing)} (Silver)")
            ])

        except Exception as e:
            timing_result = mo.md(f"❌ Erreur : {str(e)}")

    timing_result
    return (
        df_bronze_timing,
        df_silver_timing,
        gain_percent,
        speedup,
        start_b,
        start_s,
        time_bronze_exec,
        time_silver_exec,
        timing_result,
    )


if __name__ == "__main__":
    app.run()
