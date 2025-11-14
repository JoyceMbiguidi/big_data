from kafka import KafkaProducer
import json, time, os

# def send_file_to_kafka(filename, topic, producer):
#     with open(filename, "r", encoding="utf-8") as f:
#         for line in f:
#             event = json.loads(line)
#             producer.send(topic, value=event)
#             time.sleep(0.05)  # petit délai pour simuler un flux
#     print(f"{filename} envoyé dans le topic {topic}")

# if __name__ == "__main__":
#     producer = KafkaProducer(
#         bootstrap_servers="kafka:9092",
#         value_serializer=lambda v: json.dumps(v).encode("utf-8")
#     )

#     mapping = {
#         "click_events.json": "greenmarket_clicks",
#         "cart_events.json": "greenmarket_cart",
#         "purchase_events.json": "greenmarket_purchase",
#         "navigation_events.json": "greenmarket_navigation"
#     }

#     for file, topic in mapping.items():
#         path = f"C:\\Users\\joyce.mbiguidi\\Desktop\\big_data\\data_generator\\data\\raw\\{file}"
#         if os.path.exists(path):
#             send_file_to_kafka(path, topic, producer)

#     producer.close()

import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("./data_generator/data/raw/click_events.json") as f:
    for line in f:
        event = json.loads(line)
        producer.send("click_events", event)

producer.flush()
