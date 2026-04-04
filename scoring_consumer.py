from kafka import KafkaConsumer, KafkaProducer
import json

# Funkcja scoringowa
def score_transaction(tx):
    score = 0
    rules = []

    if tx.get('amount', 0) > 3000:
        score += 3
        rules.append('R1')
    if tx.get('category') == 'elektronika' and tx.get('amount', 0) > 1500:
        score += 2
        rules.append('R2')
    hour = tx.get('hour')
    if hour is not None and hour < 6:
        score += 2
        rules.append('R3')

    return score, rules

# Konsument transakcji
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Producent alertów
alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    tx['score'] = score
    tx['rules'] = rules

    if score >= 3:
        # Wysyłamy do tematu 'alerts'
        alert_producer.send('alerts', value=tx)
        #print(f"ALERT! {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']} | hour: {tx['hour']} | score: {score} | rules: {rules}")
        print(f"ALERT! {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | "
        f"{tx['category']} | hour: {tx.get('hour', 'N/A')} | score: {score} | rules: {rules}")
alert_producer.flush()
alert_producer.close()
