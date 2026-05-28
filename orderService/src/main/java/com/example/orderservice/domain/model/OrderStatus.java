package com.example.orderservice.domain.model;

public enum OrderStatus {
    PENDING,
    CONFIRMED,
    PROCESSING,
    SHIPPED,
    DELIVERED,
    CANCELLED,
    /** Terminal state written when Kafka publish fails during placeOrder. */
    FAILED
}
