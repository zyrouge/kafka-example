package io.github.zyrouge.kafka_example.worker;

import java.util.Set;

import io.github.zyrouge.kafka_example.shared.CounterDataUtils;
import io.github.zyrouge.kafka_example.worker.core.CounterConsumer;

public class Application {

    public static void main(String[] args) {
        CounterConsumer.start(Set.of(CounterDataUtils.TOPIC_LOREM, CounterDataUtils.TOPIC_IPSUM));
        Runtime.getRuntime().addShutdownHook(new Thread(CounterConsumer::closeInstance));
    }
}
