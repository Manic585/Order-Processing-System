package com.example.inventoryservice.kafka.consumer;

import com.example.inventoryservice.kafka.event.InventoryUpdatedEvent;
import com.example.inventoryservice.kafka.event.OrderCreatedEvent;
import com.example.inventoryservice.kafka.producer.InventoryEventPublisher;
import com.example.inventoryservice.service.InventoryCheckResult;
import com.example.inventoryservice.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProductService productService;
    private final InventoryEventPublisher eventPublisher;

    /**
     * Consumes order.created events published by order-service.
     *
     * Flow:
     *   1. Call ProductService.checkAndReserveInventory (wrapped in @CircuitBreaker).
     *   2. Publish InventoryUpdatedEvent with success=true or false.
     *   3. Acknowledge the Kafka record only after publish completes.
     *
     * On unrecoverable error the message is NOT acknowledged so that
     * DefaultErrorHandler retries up to 3 times, then routes to order.created.DLT.
     */
    @KafkaListener(topics = "order.created", groupId = "inventory-service-group")
    public void handleOrderCreated(
            @Payload OrderCreatedEvent event,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("event=ORDER_RECEIVED orderId={} customerId={} topic={} partition={} offset={}",
                event.orderId(), event.customerId(), topic, partition, offset);

        try {
            InventoryCheckResult result =
                    productService.checkAndReserveInventory(event.orderId(), event.items());

            InventoryUpdatedEvent response = new InventoryUpdatedEvent(
                    event.orderId(),
                    result.success(),
                    result.failureReason(),
                    Instant.now());

            // Block until the broker confirms receipt before acknowledging the inbound record.
            // This prevents a scenario where the inbound ack succeeds but the outbound publish
            // fails silently, leaving the order stuck in PENDING.
            eventPublisher.publish(response).join();

            acknowledgment.acknowledge();

            log.info("event=INVENTORY_RESPONSE_SENT orderId={} success={}",
                    event.orderId(), result.success());

        } catch (Exception e) {
            // Do NOT acknowledge: let DefaultErrorHandler retry, then route to DLT.
            log.error("event=ORDER_CONSUMER_ERROR orderId={} error={}",
                    event.orderId(), e.getMessage(), e);
            throw e;
        }
    }
}
