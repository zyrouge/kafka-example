package io.github.zyrouge.kafka_example.web_server.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.github.zyrouge.kafka_example.shared.CounterData;
import io.github.zyrouge.kafka_example.shared.CounterDataUtils;
import io.github.zyrouge.kafka_example.web_server.core.CounterProducer;
import io.github.zyrouge.kafka_example.web_server.utils.SuccessJSONResponse;

@RestController
@RequestMapping("/api/counter")
public class CounterController {

    @PostMapping("/increment")
    public SuccessJSONResponse<List<String>> incrementCounter(@RequestParam("topic") String topic,
            @RequestParam("count") int count) {
        Assert.isTrue(CounterDataUtils.isAllowedTopic(topic), "Invalid topic: " + topic);
        Assert.isTrue(count > 0, "Count must be greater than 0");
        Assert.isTrue(count <= 100, "Count must be less than or equal to 100");
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String key = UUID.randomUUID().toString();
            long createdAt = System.currentTimeMillis();
            CounterProducer.getInstance().produce(topic, new CounterData(topic, key, createdAt));
            keys.add(key);
        }
        return new SuccessJSONResponse<>(true, keys);
    }
}
