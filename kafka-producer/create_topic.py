from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
import time


def create_topic():
    topic_name = "user_actions_topic"
    # Ждем пока Kafka станет доступна
    for i in range(30):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=['kafka:9092'],
                client_id='topic_creator'
            )

            topic_list = [
                NewTopic(
                    name=topic_name,
                    num_partitions=6,
                    replication_factor=1
                )
            ]

            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{topic_name}' created successfully")
            break
        except TopicAlreadyExistsError:
            print(f"Topic '{topic_name}' already exists, skipping creation")
            break
        except Exception as e:
            error_msg = str(e).lower()
            # Проверяем, не связано ли исключение с тем, что топик уже существует
            if 'already exists' in error_msg or 'topicalreadyexists' in error_msg:
                print(f"Topic '{topic_name}' already exists, skipping creation")
                break
            print(f"Attempt {i + 1}: Kafka not ready yet - {e}")
            time.sleep(5)


if __name__ == "__main__":
    create_topic()