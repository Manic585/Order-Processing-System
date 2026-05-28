package com.example.inventoryservice.kafka.event;

import java.time.Instant;

public record InventoryUpdatedEvent(
        String orderId,
        boolean success,
        String failureReason,
        Instant timestamp
) {}
