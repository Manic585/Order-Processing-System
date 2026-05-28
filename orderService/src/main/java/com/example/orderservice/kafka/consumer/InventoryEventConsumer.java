package com.example.orderservice.kafka.consumer;

import com.example.orderservice.domain.model.OrderStatus;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.kafka.event.InventoryUpdatedEvent;
import com.example.orderservice.service.OrderService;
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
public class InventoryEventConsumer {

    private final OrderService orderService;

    /**
     * Consumes inventory.updated events published by inventory-service.
     *
     * Success path  → order transitions to CONFIRMED.
     * Failure path  → order transitions to FAILED; failureReason is logged.
     *
     * Acknowledgment is manual (MANUAL_IMMEDIATE configured in KafkaConfig).
     * The message is acknowledged only after the status update commits.
     * On unrecoverable error (e.g. OrderNotFoundException after retries),
     * the DefaultErrorHandler will forward the record to inventory.updated.DLT.
     */
    @KafkaListener(topics = "inventory.updated")
    public void handleInventoryUpdated(
            @Payload InventoryUpdatedEvent event,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("event=INVENTORY_RESPONSE orderId={} success={} topic={} partition={} offset={}",
                event.orderId(), event.success(), topic, partition, offset);

        try {
            if (event.success()) {
                orderService.updateOrderStatus(event.orderId(), OrderStatus.CONFIRMED);
                log.info("event=ORDER_CONFIRMED orderId={}", event.orderId());
            } else {
                log.warn("event=INVENTORY_RESERVATION_FAILED orderId={} reason={}",
                        event.orderId(), event.failureReason());
                orderService.updateOrderStatus(event.orderId(), OrderStatus.FAILED);
                log.info("event=ORDER_FAILED orderId={} reason={}", event.orderId(), event.failureReason());
            }

            acknowledgment.acknowledge();

        } catch (OrderNotFoundException e) {
            // The order may have been cancelled or never persisted (e.g. Kafka replay
            // of an old event).  Log and acknowledge to prevent infinite retries on a
            // record that can never succeed; rely on the DLT for audit.
            log.warn("event=INVENTORY_CONSUMER_ORDER_MISSING orderId={} error={}",
                    event.orderId(), e.getMessage());
            acknowledgment.acknowledge();

        } catch (Exception e) {
            // Do NOT acknowledge: let DefaultErrorHandler retry up to 3 times,
            // then route to inventory.updated.DLT if still failing.
            log.error("event=INVENTORY_CONSUMER_ERROR orderId={} error={}",
                    event.orderId(), e.getMessage(), e);
            throw e;
        }
    }
}
