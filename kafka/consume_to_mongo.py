import os
import json
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient

def main():
    # 1. Configuration de la connexion MongoDB (Data Lake)
    print("🔌 Connexion à MongoDB...")
    try:
        mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
        db = mongo_client["emploi_datalake"]
        collection = db["raw_jobs"]
        print("✅ Connecté à MongoDB (Base: emploi_datalake, Collection: raw_jobs)")
    except Exception as e:
        print(f"❌ Erreur de connexion à MongoDB : {e}")
        sys.exit(1)

    # 2. Configuration du Consumer Kafka
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BROKER', 'localhost:9092'),
        'group.id': 'mongodb-ingestion-group',
        'auto.offset.reset': 'earliest' # Permet de lire les anciens messages si on vient de lancer le script
    }

    consumer = Consumer(conf)
    topic = "hellowork_jobs"
    
    # S'abonner au topic
    consumer.subscribe([topic])
    print(f"🎧 En écoute sur le topic Kafka '{topic}'... (Appuyez sur Ctrl+C pour arrêter)")

    try:
        while True:
            # Polling (vérification) des nouveaux messages toutes les 1 seconde
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de la partition atteinte
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Le topic n'a pas encore été créé par le scraper, on attend...
                    continue
                elif msg.error():
                    raise KafkaException(msg.error())
                    
            # 3. Traitement du message reçu
            try:
                # Décoder le message de bytes en string, puis de string en JSON (dict)
                job_data = json.loads(msg.value().decode('utf-8'))
                
                # Insérer dans MongoDB
                result = collection.insert_one(job_data)
                
                print(f"📥 Offre ingérée dans MongoDB : {job_data.get('titre')} chez {job_data.get('entreprise')} (ID: {result.inserted_id})")
            except json.JSONDecodeError:
                print(f"⚠️ Message ignoré (format JSON invalide) : {msg.value()}")
            except Exception as e:
                print(f"❌ Erreur lors de l'insertion dans MongoDB : {e}")

    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur.")
    finally:
        # Fermer le consumer proprement pour rendre l'offset au cluster
        print("Fermeture du Consumer Kafka...")
        consumer.close()
        mongo_client.close()

if __name__ == "__main__":
    main()
