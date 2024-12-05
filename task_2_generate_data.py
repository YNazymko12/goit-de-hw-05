from confluent_kafka import Producer
from configs import kafka_config
import json
import uuid
import time
import random
from datetime import datetime
from colorama import Fore, Style, init

# Ініціалізація colorama
init(autoreset=True)

# Створення Kafka Producer
producer = Producer(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

# Функція для генерації та відправки даних від датчика
def send_sensor_data():
    print(f"{Fore.YELLOW}Starting data generation...")

    while True:
        # Генерація унікального sensor_id для кожного повідомлення
        sensor_id = str(uuid.uuid4())
        # Генерація даних
        temperature = random.uniform(25, 45)
        humidity = random.uniform(15, 85)
        timestamp = datetime.utcnow().isoformat()

        data = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "humidity": humidity,
        }

        # Виведення даних з кольоровим форматуванням
        print(f"{Fore.BLUE}----------------------------------------{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Sensor ID: {sensor_id}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}Temperature: {temperature:.2f}°C{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Humidity: {humidity:.2f}%{Style.RESET_ALL}")
        print(f"{Fore.MAGENTA}Timestamp: {timestamp}{Style.RESET_ALL}")
        print(f"{Fore.BLUE}----------------------------------------{Style.RESET_ALL}")

        # Відправка даних до топіку
        topic_name = "building_sensors"
        producer.produce(
            topic_name,
            key=sensor_id,
            value=json.dumps(data),
            callback=lambda err, msg: print(
                f"{Fore.GREEN}Message sent: {msg.value()} to {msg.topic()}{Style.RESET_ALL}"
                if err is None else f"{Fore.RED}Error: {err}{Style.RESET_ALL}"
            ),
        )

        # Очищення буфера Producer
        producer.flush()

        print(f"Data sent to Kafka topic {topic_name}")

        # Затримка перед наступною ітерацією
        time.sleep(2)

# Запуск функції
try:
    send_sensor_data()
except KeyboardInterrupt:
    print(f"{Fore.RED}\nData generation stopped by user.{Style.RESET_ALL}")
