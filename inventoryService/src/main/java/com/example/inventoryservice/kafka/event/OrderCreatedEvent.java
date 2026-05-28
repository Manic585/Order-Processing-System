package com.example.inventoryservice.kafka.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        List<OrderItemEvent> items,
        BigDecimal totalAmount,
        Instant timestamp
) {}
