package io.github.zyrouge.kafka_example.web_server.utils;

public record SuccessJSONResponse<T>(boolean success, T data) {

    public SuccessJSONResponse(boolean success) {
        this(success, null);
    }
}
