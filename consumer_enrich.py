from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    
    # Dodanie poziomu ryzyka
    if tx['amount'] > 3000:
        tx['risk_level'] = 'HIGH'
    elif tx['amount'] > 1000:
        tx['risk_level'] = 'MEDIUM'
    else:
        tx['risk_level'] = 'LOW'
    
    print(f"{tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']} | hour: {tx['hour']} | risk: {tx['risk_level']}")
