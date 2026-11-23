package io.github.zyrouge.kafka_example.shared;

public class CounterDataUtils {
    public static final String TOPIC_LOREM = "lorem";
    public static final String TOPIC_IPSUM = "ipsum";

    public static boolean isAllowedTopic(String topic) {
        return TOPIC_LOREM.equals(topic) || TOPIC_IPSUM.equals(topic);
    }
}
