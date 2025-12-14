from kafka import KafkaProducer
import json
import time
import math
import random
from datetime import datetime

'''Ce code simule un réseau de capteurs environnementaux et stream leurs données en temps réel dans Kafka.
Les données sont structurées en JSON, avec des valeurs dynamiques et des anomalies aléatoires.

'''

# --- setup du kafka ---
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8') #crée un producteur Kafka connecté au broker kafka:29092
)

# --- params de simulation ---
NUM_SENSORS = 10
t = 0  # time step globale

# --- Initialisations des sensors avec persistent state ---- on définis 10 capteurs virtuels (NUM_SENSORS = 10) avec des caractéristiques initiales
sensors = {}
for i in range(1, NUM_SENSORS + 1):
    sensors[i] = {
        "sensor_id": i,
        "device_type": "environment_sensor",
        "location_x": random.uniform(0, 100),
        "location_y": random.uniform(0, 100),
        "temperature": 20 + random.uniform(-1, 1),
        "humidity": 55 + random.uniform(-2, 2),
        "pressure": 1013 + random.uniform(-1, 1),
        "co2_level": 450 + random.uniform(-10, 10),
        "battery_level": 100,
        "status": "normal"
    }

# --- Main loop ---- chaque seconde (time.sleep(1)), les capteurs mettent à jour leurs valeurs selon des règles physiques simplifiées (cycles jour/nuit, fluctuations aléatoires, décharge batterie, anomalies).
while True:
    for sensor in sensors.values():
        # Température suit un cycle sinusoidal (jour/nuit)
        sensor["temperature"] += math.sin(t / 50) * 0.5 + random.uniform(-0.2, 0.2)
        
        # Humidité varie inversement à la température.
        sensor["humidity"] = round(55 - (sensor["temperature"] - 20) * 1.1 + random.uniform(-1, 1), 2)
        
        # small pressure fluctuation
        sensor["pressure"] += math.sin(t / 70) * 0.2 + random.uniform(-0.2, 0.2)
        
        # Pression et Co2fluctuent legerement
        sensor["co2_level"] += math.sin(t / 100) * 2 + random.uniform(-5, 5)
        
        # batterie diminue lentement
        sensor["battery_level"] = max(0, sensor["battery_level"] - 0.005 + random.uniform(-0.05, 0.05))
        
        # Anomalies simulées
        sensor["status"] = "normal"
        if random.random() < 0.015:#- ~1.5% de chance qu’un capteur passe en "warning" (température et CO₂ augmentent brutalement).
            sensor["temperature"] += random.uniform(5, 12)
            sensor["co2_level"] += random.uniform(200, 300)
            sensor["status"] = "warning"
        if random.random() < 0.005 or sensor["battery_level"] < 5: #- ~0.5% de chance qu’il passe en "malfunction", ou automatiquement si batterie < 5%.

            sensor["status"] = "malfunction"
        
        # timestamp for the message
        sensor["timestamp"] = datetime.utcnow().isoformat()
        
        # send to Kafka
        producer.send("test-topic", value=sensor) #chaque capteur envoie un message JSON vers le topic test-topic
        print("Sent:", sensor)
    
    t += 1
    time.sleep(1)
#Chaque message est un objet JSON représentant l’état d’un capteur.=>10 messages/seconde
