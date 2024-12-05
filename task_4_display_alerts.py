from confluent_kafka import Consumer
from configs import kafka_config
import json
from colorama import Fore, init

# Ініціалізація colorama
init(autoreset=True)

# Створення Kafka Consumer
consumer = Consumer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
        "group.id": "alert_group",
        "auto.offset.reset": "earliest",
    }
)

# Підписка на топіки
consumer.subscribe(["temperature_alerts", "humidity_alerts"])

print(Fore.YELLOW + "Alert consumer started...")

# Обробка сповіщень
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print(Fore.YELLOW + "No alerts received.")
            continue
        if msg.error():
            print(Fore.RED + f"Consumer error: {msg.error()}")
            continue

        try:
            alert = json.loads(msg.value().decode("utf-8"))
            print(Fore.CYAN + f"Received alert: {alert['message']} for sensor {alert['sensor_id']}")
        except json.JSONDecodeError as e:
            print(Fore.RED + f"Failed to decode JSON: {e}")
        except KeyError as e:
            print(Fore.RED + f"Missing key in alert: {e}")
except KeyboardInterrupt:
    print(Fore.MAGENTA + "Consumer interrupted by user.")
finally:
    consumer.close()
    print(Fore.GREEN + "Consumer closed.")