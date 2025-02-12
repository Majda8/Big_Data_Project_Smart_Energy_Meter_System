from kafka import KafkaConsumer
import snowflake.connector
import json

# Configuration Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'test'

# Configuration Snowflake
SNOWFLAKE_ACCOUNT = 'th61723.eu-west-3.aws'
SNOWFLAKE_USER = 'Majdouline'
SNOWFLAKE_PASSWORD = 'Isam2019.'
SNOWFLAKE_DATABASE = 'ENERGY_DB'
SNOWFLAKE_SCHEMA = 'PUBLIC'
SNOWFLAKE_TABLE = 'ENERGY_DATA'

# Connexion à Snowflake
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )

# Création de la base de données, schéma et table
def setup_snowflake():
    try:
        connection = get_snowflake_connection()
        cursor = connection.cursor()

        # Supprimer la base de données si elle existe
        print(f"Vérification de l'existence de la base de données {SNOWFLAKE_DATABASE}...")
        cursor.execute(f"DROP DATABASE IF EXISTS {SNOWFLAKE_DATABASE}")
        print(f"Base de données {SNOWFLAKE_DATABASE} supprimée.")

        # Créer la base de données
        cursor.execute(f"CREATE DATABASE {SNOWFLAKE_DATABASE}")
        print(f"Base de données {SNOWFLAKE_DATABASE} créée.")

        # Utiliser la base de données
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")

        # Supprimer le schéma si nécessaire
        cursor.execute(f"DROP SCHEMA IF EXISTS {SNOWFLAKE_SCHEMA}")
        print(f"Schéma {SNOWFLAKE_SCHEMA} supprimé.")

        # Créer le schéma
        cursor.execute(f"CREATE SCHEMA {SNOWFLAKE_SCHEMA}")
        print(f"Schéma {SNOWFLAKE_SCHEMA} créé.")

        # Créer la table
        print(f"Création de la table {SNOWFLAKE_TABLE}...")
        cursor.execute(f"""
            CREATE TABLE {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
                V STRING,
                I STRING,
                P STRING,
                E STRING,
                FP STRING,
                F STRING,
                TIMESTAMP STRING
            )
        """)
        print(f"Table {SNOWFLAKE_TABLE} créée.")
    except Exception as e:
        print(f"Erreur lors de la configuration de Snowflake : {e}")
    finally:
        cursor.close()
        connection.close()

# Insérer des données dans Snowflake
def insert_into_snowflake(data):
    try:
        connection = get_snowflake_connection()
        cursor = connection.cursor()
        
        # Utiliser la base et le schéma
        cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

        # Préparer la requête d'insertion
        query = f"""
        INSERT INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (V, I, P, E, FP, F, TIMESTAMP)
        VALUES (%(V)s, %(I)s, %(P)s, %(E)s, %(FP)s, %(F)s, %(timestamp)s)
        """
        cursor.execute(query, data)
        connection.commit()
        print(f"Données insérées dans Snowflake : {data}")
    except Exception as e:
        print(f"Erreur lors de l'insertion dans Snowflake : {e}")
    finally:
        cursor.close()
        connection.close()

# Créer un consommateur Kafka
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Consommer à partir du début si aucun offset n'est stocké
    enable_auto_commit=True
)

# Exécuter le setup initial
setup_snowflake()

# Consommer les messages Kafka et insérer dans Snowflake
for message in consumer:
    try:
        data = message.value
        print(f"Message consommé : {data}")
        insert_into_snowflake(data)
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")
