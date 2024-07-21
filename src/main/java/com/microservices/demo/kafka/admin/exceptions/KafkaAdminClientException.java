package com.microservices.demo.kafka.admin.exceptions;

public class KafkaAdminClientException extends RuntimeException {
    public KafkaAdminClientException() {
    }

    public KafkaAdminClientException(String message) {
        super(message);
    }

    public KafkaAdminClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
