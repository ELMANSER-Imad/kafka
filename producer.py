import requests
import json
from kafka import KafkaProducer
import time

# Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adresse du serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation en JSON
)

# URL de l'API Random User
API_URL = "https://randomuser.me/api/?results=10"

def fetch_users():
    """
    Récupère les utilisateurs depuis l'API Random User.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Vérifie les erreurs HTTP
        return response.json()["results"]  # Retourne la liste des utilisateurs
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return None

def send_to_kafka(users):
    """
    Envoie les utilisateurs à Kafka.
    """
    if users:
        producer.send("random_user_data", value=users)  # Envoie les données au topic Kafka
        print(f"Envoi de {len(users)} utilisateurs à Kafka")

def main():
    """
    Boucle principale pour récupérer et envoyer les données toutes les 10 secondes.
    """
    while True:
        users = fetch_users()
        if users:
            send_to_kafka(users)
        time.sleep(10)  # Attendre 10 secondes avant la prochaine requête

if __name__ == "__main__":
    main()