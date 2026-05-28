package com.example.orderservice.kafka.exception;

public class OrderEventPublishException extends RuntimeException {

    public OrderEventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
