# Запуск

```bash
./run.sh
```

## Остановка

```bash
docker-compose down
```


# Архитектура пайплайна

## Реализованный пайплайн

```
kafka-producer (имитация логов приложений) 
    → Apache Kafka 
    → Flink 
    → S3+Paimon (с партиционированием) 
    → ClickHouse (агрегации)
```

## Описание

Пайплайн для real-time аналитики действий пользователей мобильного приложения.

### Компоненты:

1. **Kafka Producer** (`kafka-producer/`)
   - Генерирует события действий пользователей (153 типа действий)
   - Имитирует ~30М устройств
   - Отправляет события в Kafka топик `user_actions_topic`

2. **Apache Kafka**
   - Принимает поток событий от producer
   - Топик: `user_actions_topic` (6 партиций)

3. **Apache Flink**
   - Читает события из Kafka
   - Записывает в S3+Paimon с партиционированием:
     - **Уровень 1**: Партиция по дате (`event_date`)
     - **Уровень 2**: Партиция по 2 часам (`event_hour`)
     - **Уровень 3**: Бакетирование по `user_id % 128` (128 бакетов)
   - Вычисляет агрегации:
     - Общее количество действий пользователя за 2 часа
     - Количество действий определенного типа за 2 часа
     - Общее количество действий за 5 минут (near real-time)
   - Записывает агрегации в ClickHouse

4. **S3 (Yandex Cloud) + Paimon**
   - Хранилище для всех событий пользователей
   - Партиционирование для оптимизации запросов
   - Bucket: `user-actions-paimon`

5. **ClickHouse**
   - Хранит агрегированные данные
   - Таблицы:
     - `user_aggregations` - агрегации за 2 часа
     - `user_aggregations_realtime` - агрегации за 5 минут (near real-time)

### Структура данных

**Событие пользователя:**
- `event_id` - уникальный ID события
- `user_id` - ID пользователя (1-30,000,000)
- `action_type` - тип действия (153 различных типа)
- `timestamp` - время события
- `metadata` - дополнительная информация (JSON)
- `device_info` - информация об устройстве (JSON)

### Агрегации

1. **2-часовые окна:**
   - Общее количество действий пользователя
   - Количество действий по типам

2. **5-минутные окна (near real-time):**
   - Общее количество действий пользователя
   - Обновление каждые 5 минут


### Файлы проекта

- [FlinkStreamingJob.java](flink-job/src/main/java/FlinkStreamingJob.java) - основной Flink job
- [kafka_producer.py](kafka-producer/kafka_producer.py) - генератор событий
- [clickhouse_schema.sql](init/clickhouse_schema.sql) - схема таблиц ClickHouse
- [docker-compose.yml](docker-compose.yml) - конфигурация Docker
