import streamlit as st
import pandas as pd
import psycopg2
from time import sleep
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pytz


SENSOR_COLORS = {
    1: '#1f77b4', 2: '#ff7f0e', 3: '#2ca02c', 4: '#d62728', 5: '#9467bd',
    6: '#8c564b', 7: '#e377c2', 8: '#7f7f7f', 9: '#bcbd22', 10: '#17becf'
}

DB_HOST = "db"
DB_NAME = "TR"
DB_USER = "postgres"
DB_PASS = "hana"
TIMEZONE = "Africa/Tunis"

st.set_page_config(page_title="capteurs environnementaux Temps Réel", layout="wide")

# --- DB Connection ---
@st.cache_resource #avec cache → une seule connexion réutilisée
def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
    except Exception as e:
        st.error(f"Erreur de connexion à la base : {e}")
        return None

conn = get_db_connection()
tz = pytz.timezone(TIMEZONE)

if conn:
    st.title("Dashboard capteurs environnementaux Temps Réel")
    #On donnes à l’utilisateur :la fenêtre temporelle qu’il veut voir, le capteur,la fréquence de rafraîchissement
    st.sidebar.header("Paramètres de Visualisation")
    time_range_minutes = st.sidebar.slider("Fenêtre Historique (Minutes)", 1, 60, 10)
    
    # Sensors filter
    sensor_ids_query = pd.read_sql("SELECT DISTINCT sensor_id FROM sensor_data ORDER BY sensor_id", conn)
    sensor_ids = sensor_ids_query['sensor_id'].tolist()
    sensor_ids.insert(0, 'Tous')
    selected_sensor = st.sidebar.selectbox("Filtrer par Capteur", sensor_ids)
    
    refresh_rate = st.sidebar.slider("Rafraîchissement (secondes)", 1, 10, 2)
    
    live_placeholder = st.empty()
    t = 0
    
    while True:
        try:
            start_time = datetime.now() - timedelta(minutes=time_range_minutes)
            
            # --- Raw Data ---
            raw_where = [f"timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'"]
            if selected_sensor != 'Tous':
                raw_where.append(f"sensor_id = {selected_sensor}")
            raw_condition = " AND ".join(raw_where)
            
            raw_df = pd.read_sql(f"""
                SELECT * FROM sensor_data
                WHERE {raw_condition}
                ORDER BY timestamp ASC
            """, conn)
            
            if not raw_df.empty:
                raw_df['timestamp'] = pd.to_datetime(raw_df['timestamp'])
                raw_df = raw_df.set_index('timestamp').sort_index()
                raw_plot = raw_df.reset_index()
            
            # --- Aggregated Data ---
            agg_df = pd.read_sql(f"""
                SELECT * FROM sensor_data_agg
                WHERE window_end >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
                {f"AND sensor_id = {selected_sensor}" if selected_sensor != 'Tous' else ""}
                ORDER BY window_end ASC
            """, conn)
            if not agg_df.empty:
                agg_df['window_start'] = pd.to_datetime(agg_df['window_start'])
                agg_df['window_end'] = pd.to_datetime(agg_df['window_end'])
            
            # Colors map
            present_sensors = raw_plot['sensor_id'].unique().tolist() if not raw_df.empty else []
            color_map = {sid: SENSOR_COLORS.get(sid, '#000000') for sid in present_sensors}
            
            with live_placeholder.container():
                st.subheader(f"Dernière mise à jour : {datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')}")
                
                # --- Tabs: Raw vs Aggregated ---
                tabs = st.tabs(["📈 Live Feed", "📊 Aggregated Trends"])
                
                # --- TAB 1: RAW DATA ---But :état instantané, anomalies,comportement fin: Que se passe-t-il maintenant ?
                with tabs[0]:
                    if raw_df.empty:
                        st.warning("Aucune donnée brute trouvée.")
                    else:
                        latest = raw_df.iloc[-1]
                    
                        #Battery kpi summary
                        if selected_sensor == 'Tous':
                            battery_mean=raw_df['battery_level'].mean()
                            battery_min=raw_df['battery_level'].min()
                            battery_max=raw_df['battery_level'].max()
                            
                            b1,b2,b3 = st.columns(3)
                            b1.metric("batterie Moyenne (%)", f"{battery_mean:.1f}")
                            b2.metric("batterie Min (%)", f"{battery_min:.1f}")
                            b3.metric("batterie Max (%)", f"{battery_max:.1f}")
                            
                            fig_batt=px.bar(raw_df, x='sensor_id', y='battery_level', color='sensor_id', title="Niveau de Batterie par Capteur")
                            st.plotly_chart(fig_batt, use_container_width=True, key=f"batt_bar_{t}")
                        else:
                        
                            col1, col2, col3, col4, col5 = st.columns(5)
                            col1.metric("Température (°C)", f"{latest['temperature']:.1f}")
                            col2.metric("Humidité (%)", f"{latest['humidity']:.1f}")
                            col3.metric("Pression (hPa)", f"{latest['pressure']:.2f}")
                            col4.metric("CO2 (ppm)", f"{latest['co2_level']:.0f}")
                            col5.metric("Batterie (%)", f"{latest['battery_level']:.1f}")   
                        
                        # Alerts
                        if latest['status'] == 'warning':
                            st.warning(f"⚠️ Capteur {latest['sensor_id']} - WARNING")
                        elif latest['status'] == 'malfunction':
                            st.error(f"🚨 Capteur {latest['sensor_id']} - PANNE")
                        
                        # Time series plots
                        graph1, graph2 = st.columns(2)
                        graph1.plotly_chart(px.line(raw_plot, x='timestamp', y='temperature', color='sensor_id', title="Température (°C)"), use_container_width=True,key=f"temp_{t}")
                        graph2.plotly_chart(px.line(raw_plot, x='timestamp', y='humidity', color='sensor_id', title="Humidité (%)"), use_container_width=True,key=f"hum_{t}")
                        
                        # CO2 & Pressure trends
                        graph3, graph4 = st.columns(2)
                        graph3.plotly_chart(px.line(raw_plot, x='timestamp', y='co2_level', color='sensor_id', title="CO2 (ppm)"), use_container_width=True,key=f"co2_{t}")
                        graph4.plotly_chart(px.line(raw_plot, x='timestamp', y='pressure', color='sensor_id', title="Pression (hPa)"), use_container_width=True,key=f"press_{t}")
                        
                        # Map
                        st.plotly_chart(px.scatter(raw_plot, x='location_x', y='location_y', color='sensor_id',
                                                   size='co2_level', hover_data=['temperature','humidity','battery_level'],
                                                   title="Carte des capteurs"), use_container_width=True, key=f"map_{t}")
                        fig_heatmap = px.density_heatmap(raw_plot, x='timestamp', y='sensor_id', z='co2_level', color_continuous_scale='Viridis', title="CO2 Levels Heatmap")
                        st.plotly_chart(fig_heatmap, use_container_width=True, key=f"co2_heatmap_{t}")
                        status_map = {'normal': 0, 'warning': 1, 'malfunction': 2}
                        raw_plot['status_numeric'] = raw_plot['status'].map(status_map)
                        fig_status = px.scatter(raw_plot, x='timestamp', y='sensor_id', color='status',size='status_numeric', size_max=20,title="Sensor Status Timeline")
                        st.plotly_chart(fig_status, use_container_width=True, key=f"status_{t}")
                
                # --- TAB 2: AGGREGATED DATA ---chaque point = une fenêtre Spark,on affiche des résultats stables,pas de bruit,pas de sur-réaction: Quelle est la tendance ?
                with tabs[1]:
                    if agg_df.empty:
                        st.warning("Aucune donnée agrégée trouvée.")
                    else:
                        # Trends
                        agg1, agg2 = st.columns(2)
                        agg1.plotly_chart(px.line(agg_df, x='window_end', y='avg_temp', color='sensor_id', title="Température Moyenne 1-min"), use_container_width=True, key=f"agg_temp_{t}")
                        agg2.plotly_chart(px.line(agg_df, x='window_end', y='avg_humidity', color='sensor_id', title="Humidité Moyenne 1-min"), use_container_width=True, key=f"agg_hum_{t}")
                        
                        agg3, agg4 = st.columns(2)
                        agg3.plotly_chart(px.line(agg_df, x='window_end', y='avg_co2', color='sensor_id', title="CO2 Moyen 1-min"), use_container_width=True, key=f"agg_co2_{t}")
                        agg4.plotly_chart(
                            px.pie(
                                agg_df.groupby('sensor_id')['warning_count'].sum().reset_index(),
                                names='sensor_id',
                                values='warning_count',
                                title="Répartition des Warnings par Capteur"
                            ),
                            use_container_width=True,
                            key=f"agg_warn_{t}"
                        )
                        # Aggregated stats table
                        st.subheader("Statistiques Agrégées par Capteur")
                        stats_df = agg_df.groupby('sensor_id')[['avg_temp','avg_humidity','avg_co2','avg_battery']].mean().reset_index()
                        st.dataframe(stats_df)
                        if not agg_df.empty:
                            corr_df = agg_df[['avg_temp','avg_humidity','avg_co2','avg_battery']].corr()
                            fig_corr = px.imshow(corr_df, text_auto=True, color_continuous_scale='RdBu_r',title="Niveaux Moyens de CO2 par Capteur")                        
                            st.plotly_chart(fig_corr, use_container_width=True, key=f"corr_{t}")

            
            t += 1
            sleep(refresh_rate)
        
        except Exception as e:
            st.error(f"Erreur mise à jour dashboard: {e}")
            sleep(5)
            conn = get_db_connection()#Si la DB tombe :le dashboard ne crash pas,il se reconnecte
