import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class FlinkStreamingJob {

    // S3 Configuration

    private static final String S3_PATH = "s3://" + S3_BUCKET + "/warehouse/paimon/";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(60000); // 1 minute checkpoints
        
        // Configure S3 filesystem
        Configuration config = new Configuration();
        config.setString("s3.access-key", S3_ACCESS_KEY);
        config.setString("s3.secret-key", S3_SECRET_KEY);
        config.setString("s3.endpoint", S3_ENDPOINT);
        config.setString("s3.path.style.access", "true");

        // Также добавьте Hadoop S3 конфигурацию
        config.setString("fs.s3a.access.key", S3_ACCESS_KEY);
        config.setString("fs.s3a.secret.key", S3_SECRET_KEY);
        config.setString("fs.s3a.endpoint", S3_ENDPOINT);
        config.setString("fs.s3a.path.style.access", "true");

        // Установите схему файловой системы
        config.setString("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        env.configure(config);

        // Create Table Environment for Paimon
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .withConfiguration(config)
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Register Paimon catalog
        tableEnv.executeSql(
            "CREATE CATALOG paimon_catalog WITH (" +
            "  'type' = 'paimon'," +
            "  's3.path.style.access' = 'true'" +
            ")"
        );

        tableEnv.executeSql("USE CATALOG paimon_catalog");

        // Create Paimon table with partitioning
        // Note: Paimon table creation syntax
        // Using dynamic bucket mode (-1) because primary key doesn't include all partition fields
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS user_actions (" +
            "  event_id STRING," +
            "  user_id BIGINT," +
            "  action_type STRING," +
            "  `timestamp` TIMESTAMP(3)," +
            "  metadata STRING," +
            "  device_info STRING," +
            "  event_date DATE," +
            "  event_hour INT," +
            "  bucket_id INT," +
            "  PRIMARY KEY (event_id, bucket_id) NOT ENFORCED" +
            ") PARTITIONED BY (event_date, event_hour) " +
            "WITH (" +
            "  'bucket' = '-1'" +
            ")"
        );

        // Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("user_actions_topic")
            .setGroupId("flink-paimon-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Create stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(
            source,
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode json = mapper.readTree(event);
                        if (json.has("timestamp")) {
                            String ts = json.get("timestamp").asText();
                            return Instant.parse(ts).toEpochMilli();
                        }
                        return System.currentTimeMillis();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                }),
            "Kafka Source"
        );

        // Parse JSON to UserAction
        DataStream<UserAction> userActionStream = kafkaStream.map(new MapFunction<String, UserAction>() {
            private transient ObjectMapper objectMapper;

            @Override
            public UserAction map(String value) throws Exception {
                try {
                    if (objectMapper == null) {
                        objectMapper = new ObjectMapper();
                    }
                    if (value == null || value.trim().isEmpty()) {
                        // Return a default UserAction for invalid input
                        UserAction action = new UserAction();
                        action.eventId = "";
                        action.userId = 0L;
                        action.actionType = "";
                        action.timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault());
                        action.metadata = "{}";
                        action.deviceInfo = "{}";
                        return action;
                    }
                    return UserAction.fromJson(objectMapper.readTree(value));
                } catch (Exception e) {
                    // Return a default UserAction on parsing error
                    UserAction action = new UserAction();
                    action.eventId = "";
                    action.userId = 0L;
                    action.actionType = "";
                    action.timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault());
                    action.metadata = "{}";
                    action.deviceInfo = "{}";
                    return action;
                }
            }
        });

        // Convert to Table and write to Paimon
        Table userActionTable = tableEnv.fromDataStream(
            userActionStream,
            org.apache.flink.table.api.Schema.newBuilder()
                .column("eventId", "STRING")
                .column("userId", "BIGINT")
                .column("actionType", "STRING")
                .column("timestamp", "TIMESTAMP(3)")
                .column("metadata", "STRING")
                .column("deviceInfo", "STRING")
                .columnByExpression("event_date", "CAST(`timestamp` AS DATE)")
                .columnByExpression("event_hour", "CAST(FLOOR(HOUR(`timestamp`) / 2.0) * 2 AS INT)")
                .columnByExpression("bucket_id", "CAST(`userId` % 128 AS INT)")
                .build()
        );
        tableEnv.createTemporaryView("user_actions_stream", userActionTable);

        // Insert into Paimon table
        tableEnv.executeSql(
            "INSERT INTO user_actions " +
            "SELECT " +
            "  eventId as event_id, " +
            "  userId as user_id, " +
            "  actionType as action_type, " +
            "  `timestamp`, " +
            "  CAST(metadata AS STRING) as metadata, " +
            "  CAST(deviceInfo AS STRING) as device_info, " +
            "  event_date, " +
            "  event_hour, " +
            "  bucket_id " +
            "FROM user_actions_stream"
        );

        // Calculate aggregations and write to ClickHouse
        calculateAggregations(userActionStream, env);

        env.execute("User Actions Pipeline: Kafka -> Paimon -> ClickHouse");
    }

    private static void calculateAggregations(DataStream<UserAction> stream, StreamExecutionEnvironment env) {
        // 1. Total actions per user in 2-hour windows
        stream
            .keyBy(action -> action.userId)
            .window(TumblingEventTimeWindows.of(Time.minutes(3)))
            .process(new ProcessWindowFunction<UserAction, UserAggregation, Long, TimeWindow>() {
                @Override
                public void process(
                    Long userId,
                    Context context,
                    Iterable<UserAction> elements,
                    Collector<UserAggregation> out
                ) throws Exception {
                    int totalActions = 0;
                    for (UserAction action : elements) {
                        totalActions++;
                    }
                    
                    UserAggregation agg = new UserAggregation();
                    agg.userId = userId;
                    agg.windowStart = new java.sql.Timestamp(context.window().getStart());
                    agg.windowEnd = new java.sql.Timestamp(context.window().getEnd());
                    agg.totalActions = totalActions;
                    agg.aggregationType = "total_actions_2h";
                    
                    out.collect(agg);
                }
            })
            .addSink(JdbcSink.sink(
                "INSERT INTO user_aggregations (user_id, window_start, window_end, total_actions, aggregation_type, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, now())",
                (statement, agg) -> {
                    statement.setLong(1, agg.userId);
                    statement.setTimestamp(2, agg.windowStart);
                    statement.setTimestamp(3, agg.windowEnd);
                    statement.setInt(4, agg.totalActions);
                    statement.setString(5, agg.aggregationType);
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(5000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:clickhouse://clickhouse:8123/mydb")
                    .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                    .withUsername("admin")
                    .withPassword("password")
                    .build()
            ));

        // 2. Specific action types per user in 2-hour windows
        stream
            .keyBy(action -> action.userId + "|" + action.actionType)
            .window(TumblingEventTimeWindows.of(Time.minutes(2)))
            .process(new ProcessWindowFunction<UserAction, UserAggregation, String, TimeWindow>() {
                @Override
                public void process(
                    String key,
                    Context context,
                    Iterable<UserAction> elements,
                    Collector<UserAggregation> out
                ) throws Exception {
                    String[] parts = key.split("\\|", 2);
                    long userId = Long.parseLong(parts[0]);
                    String actionType = parts.length > 1 ? parts[1] : "";
                    
                    int count = 0;
                    for (UserAction action : elements) {
                        count++;
                    }
                    
                    UserAggregation agg = new UserAggregation();
                    agg.userId = userId;
                    agg.windowStart = new java.sql.Timestamp(context.window().getStart());
                    agg.windowEnd = new java.sql.Timestamp(context.window().getEnd());
                    agg.totalActions = count;
                    agg.aggregationType = "action_type_" + actionType + "_2h";
                    
                    out.collect(agg);
                }
            })
            .addSink(JdbcSink.sink(
                "INSERT INTO user_aggregations (user_id, window_start, window_end, total_actions, aggregation_type, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, now())",
                (statement, agg) -> {
                    statement.setLong(1, agg.userId);
                    statement.setTimestamp(2, agg.windowStart);
                    statement.setTimestamp(3, agg.windowEnd);
                    statement.setInt(4, agg.totalActions);
                    statement.setString(5, agg.aggregationType);
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(5000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:clickhouse://clickhouse:8123/mydb")
                    .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                    .withUsername("admin")
                    .withPassword("password")
                    .build()
            ));

        // 3. 5-minute windows for near real-time aggregations
        stream
            .keyBy(action -> action.userId)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .process(new ProcessWindowFunction<UserAction, UserAggregation, Long, TimeWindow>() {
                @Override
                public void process(
                    Long userId,
                    Context context,
                    Iterable<UserAction> elements,
                    Collector<UserAggregation> out
                ) throws Exception {
                    int totalActions = 0;
                    for (UserAction action : elements) {
                        totalActions++;
                    }
                    
                    UserAggregation agg = new UserAggregation();
                    agg.userId = userId;
                    agg.windowStart = new java.sql.Timestamp(context.window().getStart());
                    agg.windowEnd = new java.sql.Timestamp(context.window().getEnd());
                    agg.totalActions = totalActions;
                    agg.aggregationType = "total_actions_5m";
                    
                    out.collect(agg);
                }
            })
            .addSink(JdbcSink.sink(
                "INSERT INTO user_aggregations_realtime (user_id, window_start, window_end, total_actions, aggregation_type, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, now())",
                (statement, agg) -> {
                    statement.setLong(1, agg.userId);
                    statement.setTimestamp(2, agg.windowStart);
                    statement.setTimestamp(3, agg.windowEnd);
                    statement.setInt(4, agg.totalActions);
                    statement.setString(5, agg.aggregationType);
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(1000)
                    .withBatchIntervalMs(5000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl("jdbc:clickhouse://clickhouse:8123/mydb")
                    .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                    .withUsername("admin")
                    .withPassword("password")
                    .build()
            ));
    }

    // UserAction class
    public static class UserAction {
        public String eventId;
        public Long userId;
        public String actionType;
        public java.time.LocalDateTime timestamp;
        public String metadata;
        public String deviceInfo;

        public static UserAction fromJson(JsonNode json) {
            UserAction action = new UserAction();
            ObjectMapper mapper = new ObjectMapper();
            
            action.eventId = json.has("event_id") && !json.get("event_id").isNull() 
                ? json.get("event_id").asText() : "";
            action.userId = json.has("user_id") && !json.get("user_id").isNull() 
                ? json.get("user_id").asLong() : 0L;
            action.actionType = json.has("action_type") && !json.get("action_type").isNull() 
                ? json.get("action_type").asText() : "";
            
            if (json.has("timestamp") && !json.get("timestamp").isNull()) {
                String timestampStr = json.get("timestamp").asText();
                try {
                    // Handle ISO 8601 format with or without timezone
                    if (timestampStr.contains("Z") || timestampStr.contains("+") || timestampStr.endsWith("-")) {
                        // ISO 8601 format with timezone
                        Instant instant = Instant.parse(timestampStr);
                        action.timestamp = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                    } else {
                        // Try parsing as LocalDateTime
                        String cleanTimestamp = timestampStr.length() > 19 ? timestampStr.substring(0, 19) : timestampStr;
                        action.timestamp = LocalDateTime.parse(cleanTimestamp);
                    }
                } catch (Exception e) {
                    action.timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault());
                }
            } else {
                action.timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault());
            }
            
            // Convert JSON nodes to strings
            try {
                if (json.has("metadata") && !json.get("metadata").isNull()) {
                    action.metadata = mapper.writeValueAsString(json.get("metadata"));
                } else {
                    action.metadata = "{}";
                }
            } catch (Exception e) {
                action.metadata = "{}";
            }
            
            try {
                if (json.has("device_info") && !json.get("device_info").isNull()) {
                    action.deviceInfo = mapper.writeValueAsString(json.get("device_info"));
                } else {
                    action.deviceInfo = "{}";
                }
            } catch (Exception e) {
                action.deviceInfo = "{}";
            }
            
            return action;
        }
    }

    // Aggregation result class
    public static class UserAggregation {
        public Long userId;
        public java.sql.Timestamp windowStart;
        public java.sql.Timestamp windowEnd;
        public Integer totalActions;
        public String aggregationType;
    }
}
