package com.example.notificationservice.dto;

import java.time.Instant;

/**
 * Payload written to SQS queues.
 * Serialized as JSON by SqsTemplate's default Jackson converter.
 */
public record NotificationMessage(
        String orderId,
        String customerId,
        NotificationType type,
        String message,
        Instant timestamp
) {}
