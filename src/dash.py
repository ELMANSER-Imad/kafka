import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import plotly.express as px
from datetime import datetime
import time

# Configuration de la connexion à Cassandra
def connect_to_cassandra():
    cluster = Cluster(['localhost'])  # Adresse de votre cluster Cassandra
    session = cluster.connect('user_keyspace')  # Keyspace défini dans votre script PySpark
    return session, cluster

# Récupération des données brutes
def fetch_raw_users(session, limit=100):
    query = f"SELECT user_id, full_name, gender, age, country, ingestion_time FROM raw_users LIMIT {limit}"
    rows = session.execute(SimpleStatement(query))
    return pd.DataFrame(rows)

# Récupération des statistiques par pays
def fetch_country_stats(session):
    query = "SELECT country, count_users, avg_age, last_update FROM country_stats"
    rows = session.execute(SimpleStatement(query))
    return pd.DataFrame(rows)

# Mise en cache des données pour éviter des requêtes répétées
@st.cache_data(ttl=20)  # Rafraîchissement toutes les 60 secondes
def load_data():
    session, cluster = connect_to_cassandra()
    raw_users_df = fetch_raw_users(session)
    country_stats_df = fetch_country_stats(session)
    cluster.shutdown()
    return raw_users_df, country_stats_df

# Interface Streamlit
def main():
    st.title("Tableau de Bord - Streaming Utilisateurs")

    # Chargement des données
    st.write("Connexion à Cassandra et chargement des données...")
    raw_users_df, country_stats_df = load_data()

    # Section 1 : Données brutes des utilisateurs
    st.subheader("Derniers Utilisateurs")
    st.write("Affiche les données brutes des utilisateurs ingérées depuis Kafka.")

    # Filtres interactifs
    countries = raw_users_df['country'].unique().tolist()
    selected_country = st.multiselect("Filtrer par pays", options=countries, default=countries)
    age_range = st.slider("Filtrer par âge", min_value=0, max_value=100, value=(0, 100))

    # Application des filtres
    filtered_users = raw_users_df[
        (raw_users_df['country'].isin(selected_country)) &
        (raw_users_df['age'].between(age_range[0], age_range[1]))
    ]
    st.dataframe(filtered_users, use_container_width=True)

    # Section 2 : Statistiques agrégées par pays
    st.subheader("Statistiques par Pays")
    st.write("Affiche le nombre d'utilisateurs et l'âge moyen par pays.")

    # Tableau des stats
    st.dataframe(country_stats_df, use_container_width=True)

    # Graphique : Nombre d'utilisateurs par pays
    fig_users = px.bar(
        country_stats_df,
        x="country",
        y="count_users",
        title="Nombre d'utilisateurs par pays",
        labels={"count_users": "Nombre d'utilisateurs", "country": "Pays"}
    )
    st.plotly_chart(fig_users, use_container_width=True)

    # Graphique : Âge moyen par pays
    fig_age = px.bar(
        country_stats_df,
        x="country",
        y="avg_age",
        title="Âge moyen par pays",
        labels={"avg_age": "Âge moyen", "country": "Pays"}
    )
    st.plotly_chart(fig_age, use_container_width=True)

    time.sleep(20)  # Attendre 2 minutes
    st.rerun()  # Mise à jour automatique toutes les 120 secondes


    # Pied de page
    st.write(f"Dernière mise à jour : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()