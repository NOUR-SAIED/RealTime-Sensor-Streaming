import streamlit as st
import pandas as pd
import psycopg2
from time import sleep
import plotly.express as px
from datetime import datetime, timedelta
import pytz
# Palette de couleurs statique pour les capteurs
SENSOR_COLORS = {
    1: '#1f77b4',  # Bleu
    2: '#ff7f0e',  # Orange
    3: '#2ca02c',  # Vert
    4: '#d62728',  # Rouge
    5: '#9467bd',  # Violet
    
}

# --- Configuration et Connexion PostgreSQL ---
# Le nom d'hôte 'db' est le nom du service PostgreSQL dans docker-compose.yml
DB_HOST = "db"
DB_NAME = "TR"
DB_USER = "postgres"
DB_PASS = "hana"

st.set_page_config(
    page_title="Dashboard Capteurs Temps Réel",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Fonction de connexion unique
@st.cache_resource
def get_db_connection():
    """Initialise et retourne la connexion à PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return None

conn = get_db_connection()

if conn:
    st.title("Flux de Données Temps Réel des Capteurs")

    # --- Sidebar pour les Filtres ---
    st.sidebar.header("Options de Filtre & Rafraîchissement")

    # Filtre sur le nombre de lignes
    limit = st.sidebar.slider("Nombre de points affichés", 20, 500, 100)

    # Filtre sur l'ID du Capteur
    # Récupérer la liste des sensor_id distincts
    sensor_ids_query = pd.read_sql("SELECT DISTINCT sensor_id FROM sensor_data ORDER BY sensor_id", conn)
    sensor_ids = sensor_ids_query['sensor_id'].tolist()
    sensor_ids.insert(0, 'Tous')
    selected_sensor = st.sidebar.selectbox("Filtrer par Capteur ID", sensor_ids)
    
    # Bouton de rafraîchissement
    refresh_rate = st.sidebar.slider("Taux de Rafraîchissement (secondes)", 1, 10, 2)


    # --- Boucle de Rafraîchissement ---
    # Créer un conteneur pour mettre à jour les éléments sans recharger la page entière
    live_data_placeholder = st.empty()
    
    while True:
        try:
            # 1. Requête SQL Dynamique
            where_clause = ""
            if selected_sensor != 'Tous':
                where_clause = f"WHERE sensor_id = {selected_sensor}"
            
            sql_query = f"""
                SELECT sensor_id, temperature, humidity, timestamp 
                FROM sensor_data 
                {where_clause}
                ORDER BY timestamp DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(sql_query, conn)
            
            # Assurer que 'timestamp' est un index pour la ligne de temps
            df = df.set_index('timestamp').sort_index()

            # Mettre à jour le conteneur en direct
            with live_data_placeholder.container():
                
                # TITRE DE LA MISE À JOUR
                tz=pytz.timezone("Africa/Tunis")
                st.subheader(f"Dernière Mise à Jour : {datetime.now(tz).strftime('%H:%M:%S')}")

                # --- 2. Indicateurs Clés (Metrics) ---
                col1, col2, col3, col4 = st.columns(4)
                
                if not df.empty:
                    latest_temp = df['temperature'].iloc[-1]
                    latest_hum = df['humidity'].iloc[-1]
                    avg_temp = df['temperature'].mean()
                    total_count = df.shape[0]

                    col1.metric("Dernière Température (°C)", f"{latest_temp:.1f}")
                    col2.metric("Dernière Humidité (%)", f"{latest_hum:.1f}")
                    col3.metric("Temp. Moyenne sur {limit} points (°C)", f"{avg_temp:.1f}")
                    col4.metric("Total Points Affichés", total_count)
                else:
                    st.warning("Aucune donnée trouvée pour les filtres sélectionnés.")
                    
                
                # --- 3. Visualisations (Graphs) ---
                graph_col1, graph_col2 = st.columns(2)
                
                df_plot = df.reset_index()
                
                # --- NOUVEAU: Créer la map de couleurs en filtrant seulement les capteurs présents ---
                # Cela garantit que seuls les IDs présents dans le DF sont utilisés
                present_sensor_ids = df_plot['sensor_id'].unique().tolist()
                
                # Créer une map de couleurs filtrée
                color_map = {
                    sid: SENSOR_COLORS.get(sid, '#000000') # Utilise le noir par défaut si l'ID n'est pas dans la map
                    for sid in present_sensor_ids
                }
                
                # Graphique Température
                with graph_col1:
                    fig_temp = px.line(
                        df_plot,
                        x='timestamp', 
                        y='temperature', 
                        color='sensor_id', 
                        title="Température (°C) - Tendance Temporelle",
                        template="plotly_white",
                        # --- CLÉ DE LA CONSISTANCE ---
                        color_discrete_map=color_map 
                    )
                    st.plotly_chart(fig_temp, use_container_width=True)

                # Graphique Humidité
                with graph_col2:
                    fig_hum = px.line(
                        df_plot,
                        x='timestamp', 
                        y='humidity', 
                        color='sensor_id', 
                        title="Humidité (%) - Tendance Temporelle",
                        template="plotly_white",
                        # --- CLÉ DE LA CONSISTANCE ---
                        color_discrete_map=color_map
                    )
                    st.plotly_chart(fig_hum, use_container_width=True)

                # --- 4. Données Brutes ---
                st.subheader(f"Données Brutes (Top {limit} lignes)")
                st.dataframe(df.tail(limit).reset_index().sort_values(by='timestamp', ascending=False), use_container_width=True)

            # Pause avant la prochaine itération
            sleep(refresh_rate)

        except Exception as e:
            st.error(f"Une erreur s'est produite lors de la mise à jour du tableau de bord: {e}")
            sleep(5)
            # Tente de rétablir la connexion
            conn = get_db_connection()