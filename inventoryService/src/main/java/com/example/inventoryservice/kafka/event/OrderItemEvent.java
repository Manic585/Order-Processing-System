package com.example.inventoryservice.kafka.event;

import java.math.BigDecimal;

public record OrderItemEvent(
        String productId,
        String productName,
        int quantity,
        BigDecimal unitPrice
) {}
