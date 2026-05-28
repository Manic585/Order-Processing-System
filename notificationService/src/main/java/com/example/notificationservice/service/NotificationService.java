package com.example.notificationservice.service;

import com.example.notificationservice.dto.NotificationMessage;
import com.example.notificationservice.dto.NotificationType;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final SqsTemplate sqsTemplate;

    @Value("${notification.queue.order-confirmed}")
    private String orderConfirmedQueueUrl;

    @Value("${notification.queue.order-cancelled}")
    private String orderCancelledQueueUrl;

    @Value("${notification.queue.order-failed}")
    private String orderFailedQueueUrl;

    // ── Order confirmed ───────────────────────────────────────────────────────

    public void sendOrderConfirmation(String orderId, String customerId) {
        NotificationMessage msg = new NotificationMessage(
                orderId,
                customerId,
                NotificationType.CONFIRMED,
                "Your order " + orderId + " has been confirmed and is being processed.",
                Instant.now());

        sqsTemplate.send(orderConfirmedQueueUrl, msg);
        log.info("event=NOTIFICATION_SENT type=CONFIRMED orderId={} customerId={}",
                orderId, customerId);
    }

    // ── Order cancelled ───────────────────────────────────────────────────────

    public void sendOrderCancellation(String orderId, String customerId) {
        NotificationMessage msg = new NotificationMessage(
                orderId,
                customerId,
                NotificationType.CANCELLED,
                "Your order " + orderId + " has been cancelled.",
                Instant.now());

        sqsTemplate.send(orderCancelledQueueUrl, msg);
        log.info("event=NOTIFICATION_SENT type=CANCELLED orderId={} customerId={}",
                orderId, customerId);
    }

    // ── Order failed ──────────────────────────────────────────────────────────

    public void sendOrderFailure(String orderId, String customerId, String reason) {
        NotificationMessage msg = new NotificationMessage(
                orderId,
                customerId,
                NotificationType.FAILED,
                "Your order " + orderId + " could not be completed. Reason: " + reason,
                Instant.now());

        sqsTemplate.send(orderFailedQueueUrl, msg);
        log.info("event=NOTIFICATION_SENT type=FAILED orderId={} customerId={} reason={}",
                orderId, customerId, reason);
    }
}
