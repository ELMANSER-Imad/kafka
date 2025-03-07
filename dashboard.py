import streamlit as st
from cassandra.cluster import Cluster
import pandas as pd

# Connexion à Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('random_user_keyspace')

# Récupération des derniers utilisateurs
def get_latest_users():
    query = "SELECT * FROM random_user_table ORDER BY ingestion_time DESC LIMIT 10"
    rows = session.execute(query)
    return pd.DataFrame(rows)

# Récupération des statistiques par pays
def get_country_stats():
    query = "SELECT * FROM country_stats"
    rows = session.execute(query)
    return pd.DataFrame(rows)

# Affichage du tableau de bord
st.title("Tableau de Bord des Utilisateurs en Temps Réel")

st.header("Derniers Utilisateurs")
latest_users = get_latest_users()
st.write(latest_users)

st.header("Statistiques par Pays")
country_stats = get_country_stats()
st.bar_chart(country_stats.set_index("country")["count_users"])
st.line_chart(country_stats.set_index("country")["avg_age"])