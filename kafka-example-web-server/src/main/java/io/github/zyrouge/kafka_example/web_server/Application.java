package io.github.zyrouge.kafka_example.web_server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.github.zyrouge.kafka_example.web_server.core.CounterProducer;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        CounterProducer.getInstance();
        Runtime.getRuntime().addShutdownHook(new Thread(CounterProducer::closeInstance));
    }
}
