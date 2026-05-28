package com.example.notificationservice.kafka.consumer;

import com.example.notificationservice.kafka.event.OrderCreatedEvent;
import com.example.notificationservice.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final NotificationService notificationService;

    /**
     * Bridges order.created Kafka events to the SQS order-confirmed queue.
     *
     * SQS delivery is best-effort: if SQS is unavailable the error is logged
     * but the Kafka record is still acknowledged so that the Kafka consumer
     * does not stall or route to DLT over a transient downstream issue.
     * The trade-off is at-most-once SQS delivery for this notification type.
     */
    @KafkaListener(topics = "order.created", groupId = "notification-service-group")
    public void handleOrderCreated(
            @Payload OrderCreatedEvent event,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("event=ORDER_CREATED_RECEIVED orderId={} customerId={} topic={} partition={} offset={}",
                event.orderId(), event.customerId(), topic, partition, offset);

        try {
            notificationService.sendOrderConfirmation(event.orderId(), event.customerId());
        } catch (Exception e) {
            // SQS failure is non-blocking: acknowledge Kafka record regardless.
            log.error("event=SQS_SEND_FAILED type=CONFIRMED orderId={} error={}",
                    event.orderId(), e.getMessage(), e);
        }

        acknowledgment.acknowledge();
    }

    /**
     * Bridges order.cancelled Kafka events to the SQS order-cancelled queue.
     * Same best-effort SQS delivery semantics as handleOrderCreated.
     */
    @KafkaListener(topics = "order.cancelled", groupId = "notification-service-group")
    public void handleOrderCancelled(
            @Payload OrderCreatedEvent event,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("event=ORDER_CANCELLED_RECEIVED orderId={} customerId={} topic={} partition={} offset={}",
                event.orderId(), event.customerId(), topic, partition, offset);

        try {
            notificationService.sendOrderCancellation(event.orderId(), event.customerId());
        } catch (Exception e) {
            log.error("event=SQS_SEND_FAILED type=CANCELLED orderId={} error={}",
                    event.orderId(), e.getMessage(), e);
        }

        acknowledgment.acknowledge();
    }
}
