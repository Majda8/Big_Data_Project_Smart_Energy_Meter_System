from flask import Flask, request, jsonify
from kafka import KafkaProducer
import datetime
import json

app = Flask(__name__)

# Configuration de la connexion Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Remplacez par l'adresse de votre broker Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Définissez votre topic Kafka
TOPIC_NAME = 'test'

@app.route('/post-data', methods=['GET'])
def post_data():
    # Récupérer les paramètres depuis la requête GET
    V = request.args.get('V')
    I = request.args.get('I')
    P = request.args.get('P')
    E = request.args.get('E')
    FP = request.args.get('FP')
    F = request.args.get('F')
    current_time = datetime.datetime.now().isoformat()

    # Préparer les données pour Kafka
    data = {
        "V": float(V) if V else None,
        "I": float(I) if I else None,
        "P": float(P) if P else None,
        "E": float(E) if E else None,
        "FP": float(FP) if FP else None,
        "F": float(F) if F else None,
        "timestamp": current_time
}

    
    # Envoyer les données au topic Kafka
    try:
        producer.send(TOPIC_NAME, value=data)
        producer.flush()  # Assure l'envoi immédiat des données
        print(f"Data sent to Kafka topic '{TOPIC_NAME}': {data}")
        return jsonify({"message": "Data sent to Kafka successfully"}), 200
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)