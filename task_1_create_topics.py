from colorama import Fore, Style, init
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Ініціалізація colorama
init(autoreset=True)

# Ім'я для унікальності топіків
user_name = "yuliia"

# Список топіків для створення
topics = [
    f"{user_name}_building_sensors",
    f"{user_name}_temperature_alerts",
    f"{user_name}_humidity_alerts",
]

# Максимальна кількість топіків для виводу
MAX_TOPICS_DISPLAY = 10

# Створення клієнта Kafka
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    print(Fore.GREEN + "✅ KafkaAdminClient successfully initialized.\n")
except Exception as e:
    print(Fore.RED + f"❌ Failed to initialize KafkaAdminClient: {e}")
    exit()

# Отримання списку існуючих топіків
try:
    existing_topics = admin_client.list_topics()
    print(Fore.GREEN + "📜 Retrieved existing topics successfully.\n")
except Exception as e:
    print(Fore.RED + f"❌ Failed to retrieve existing topics: {e}")
    exit()

# Створення нових топіків
new_topics = [
    NewTopic(topic, num_partitions=1, replication_factor=1) 
    for topic in topics if topic not in existing_topics
]

# Результат створення топіків
print(Fore.GREEN + "==== Created Topics ====")
try:
    if new_topics:
        create_results = admin_client.create_topics(new_topics)
        for topic, future in create_results.items():
            try:
                future.result()  # Перевіряємо результат створення
                print(f"{Fore.CYAN}✅ {topic}")
            except Exception as e:
                print(f"{Fore.RED}❌ Failed to create topic {topic}: {e}")
    else:
        for topic in topics:
            print(f"{Fore.YELLOW}✅ {topic} (already exists)")
except Exception as e:
    print(Fore.RED + f"❌ An error occurred during topic creation: {e}")

# Виведення обмеженого списку існуючих топіків
print(Fore.GREEN + "\n==== All Existing Topics (Limited Display) ====")
limited_topics = list(existing_topics)[:MAX_TOPICS_DISPLAY]
for topic in limited_topics:
    if topic in topics:
        print(f"{Fore.YELLOW}✅ {topic} (created)")
    else:
        print(f"{Fore.MAGENTA}ℹ️  {topic}")
if len(existing_topics) > MAX_TOPICS_DISPLAY:
    print(Fore.CYAN + f"\n...and {len(existing_topics) - MAX_TOPICS_DISPLAY} more topics not displayed.")

# Закриття клієнта
print(Fore.GREEN + "\n🔒 KafkaAdminClient connection closed.")
