package com.example.inventoryservice.kafka.producer;

import com.example.inventoryservice.kafka.event.InventoryUpdatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventPublisher {

    private static final String TOPIC = "inventory.updated";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes an InventoryUpdatedEvent to inventory.updated.
     * The orderId is used as the partition key so all events for the same order
     * land on the same partition and preserve ordering.
     *
     * Returns the CompletableFuture from KafkaTemplate so callers can decide
     * whether to block (.join()) or fire-and-forget.
     */
    public CompletableFuture<SendResult<String, Object>> publish(InventoryUpdatedEvent event) {
        log.info("event=INVENTORY_PUBLISH_ATTEMPT orderId={} success={} topic={}",
                event.orderId(), event.success(), TOPIC);

        return kafkaTemplate.send(TOPIC, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("event=INVENTORY_PUBLISH_OK orderId={} partition={} offset={}",
                                event.orderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("event=INVENTORY_PUBLISH_FAILED orderId={} error={}",
                                event.orderId(), ex.getMessage(), ex);
                    }
                });
    }
}
