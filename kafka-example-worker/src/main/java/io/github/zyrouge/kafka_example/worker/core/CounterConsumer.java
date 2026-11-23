package io.github.zyrouge.kafka_example.worker.core;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.github.zyrouge.kafka_example.shared.CounterData;

public class CounterConsumer implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(CounterConsumer.class.getName());
    private static CounterConsumer INSTANCE;

    private final KafkaConsumer<String, String> consumer;

    public CounterConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void consumeAsync(Set<String> topics) {
        try {
            consumer.subscribe(topics);
            while (true) {
                for (ConsumerRecord<String, String> x : consumer.poll(Duration.ofMillis(100))) {
                    processRecord(x);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to consume counter data", e);
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            CounterData data = CounterData.fromJsonString(record.value());
            LOGGER.info(
                    "Consumed record with topic: " + data.topic() + ", key: " + data.key() + ", createdAt: "
                            + data.createdAt());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to process record", e);
        }
    }

    private static synchronized CounterConsumer getInstance() {
        if (INSTANCE == null) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "counter-consumer-group-" + System.currentTimeMillis());
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            INSTANCE = new CounterConsumer(consumer);
        }
        return INSTANCE;
    }

    public static void start(Set<String> topics) {
        try {
            CounterConsumer consumer = getInstance();
            consumer.consumeAsync(topics);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start consuming counter data", e);
        }
    }

    @Override
    public void close() {
        consumer.close();
    }

    public static synchronized void closeInstance() {
        if (INSTANCE != null) {
            INSTANCE.close();
            INSTANCE = null;
        }
    }
}
