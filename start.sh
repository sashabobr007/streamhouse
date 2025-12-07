
#!/bin/bash

CONTAINER_NAME="jobmanager"
JAR_PATH="/opt/flink/usrlib/flink-kafka-paimon-clickhouse-1.0-SNAPSHOT.jar"
MAIN_CLASS="FlinkStreamingJob"

echo "Запуск Flink job в контейнере $CONTAINER_NAME..."

# Запускаем задачу и сохраняем вывод для получения JobID
JOB_OUTPUT=$(docker exec $CONTAINER_NAME /opt/flink/bin/flink run -d -c $MAIN_CLASS $JAR_PATH)

echo "Результат запуска:"
echo "$JOB_OUTPUT"

# Извлекаем JobID из вывода
JOB_ID=$(echo "$JOB_OUTPUT" | grep "Job has been submitted with JobID" | awk '{print $NF}' | tr -d '\n\r')

if [ -z "$JOB_ID" ]; then
    # Альтернативный способ извлечения JobID
    JOB_ID=$(echo "$JOB_OUTPUT" | grep "Submitted job:" | awk '{print $NF}' | tr -d '\n\r')
fi

if [ -z "$JOB_ID" ]; then
    echo "Не удалось извлечь JobID. Пытаемся найти задачу через список..."
    # Если не удалось извлечь JobID, ищем в списке задач
    JOB_ID=$(docker exec $CONTAINER_NAME /opt/flink/bin/flink list | head -3 | tail -1 | awk '{print $4}')
fi

if [ -z "$JOB_ID" ]; then
    echo "Ошибка: не удалось определить JobID задачи"
    exit 1
fi

echo "Задача запущена с JobID: $JOB_ID"
echo "Пайплайн работает: Kafka -> Flink -> S3+Paimon -> ClickHouse"
echo "Для просмотра статуса: docker exec $CONTAINER_NAME /opt/flink/bin/flink list"
echo "Для остановки: docker exec $CONTAINER_NAME /opt/flink/bin/flink cancel $JOB_ID"