from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    # Aktualizacja liczników
    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    # Co 10 wiadomości wypisz podsumowanie
    if msg_count % 10 == 0:
        print("\n--- Podsumowanie po", msg_count, "transakcjach ---")
        print(f"{'Sklep':<10} {'Liczba':<8} {'Suma kwot (PLN)':<15}")
        for s in store_counts:
            print(f"{s:<10} {store_counts[s]:<8} {total_amount[s]:<15.2f}")
