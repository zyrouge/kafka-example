package io.github.zyrouge.kafka_example.web_server.core;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.github.zyrouge.kafka_example.shared.CounterData;

public class CounterProducer implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(CounterProducer.class.getName());
    private static CounterProducer INSTANCE;

    private final KafkaProducer<String, String> producer;

    public CounterProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public void produce(String topic, CounterData data) {
        try {
            producer.send(new ProducerRecord<>(topic, data.key(), data.toJsonString()));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to produce counter data", e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    public static synchronized CounterProducer getInstance() {
        if (INSTANCE == null) {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
            INSTANCE = new CounterProducer(producer);
        }
        return INSTANCE;
    }

    public static synchronized void closeInstance() {
        if (INSTANCE != null) {
            INSTANCE.close();
            INSTANCE = null;
        }
    }
}
