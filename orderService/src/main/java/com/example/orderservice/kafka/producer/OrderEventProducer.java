package com.example.orderservice.kafka.producer;

import com.example.orderservice.kafka.event.OrderCreatedEvent;
import com.example.orderservice.kafka.exception.OrderEventPublishException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    static final String ORDER_CREATED_TOPIC   = "order.created";
    static final String ORDER_CANCELLED_TOPIC = "order.cancelled";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes an OrderCreatedEvent to the order.created topic.
     * The orderId is used as the partition key so all events for the same
     * order land on the same partition and preserve ordering.
     *
     * @return CompletableFuture that completes with the SendResult on success,
     *         or completes exceptionally with OrderEventPublishException on failure.
     */
    public CompletableFuture<SendResult<String, Object>> publishOrderCreated(OrderCreatedEvent event) {
        return send(ORDER_CREATED_TOPIC, event.orderId(), event);
    }

    /**
     * Publishes an order-cancelled notification to the order.cancelled topic.
     * Re-uses OrderCreatedEvent to carry the original order payload alongside
     * the cancellation signal (topic name conveys the intent).
     */
    public CompletableFuture<SendResult<String, Object>> publishOrderCancelled(OrderCreatedEvent event) {
        return send(ORDER_CANCELLED_TOPIC, event.orderId(), event);
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private CompletableFuture<SendResult<String, Object>> send(
            String topic, String key, Object payload) {

        return kafkaTemplate.send(topic, key, payload)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("event=ORDER_PUBLISH_FAILED orderId={} topic={} error={}",
                                key, topic, ex.getMessage());
                    } else {
                        RecordMetadata meta = result.getRecordMetadata();
                        log.info("event=ORDER_PUBLISHED orderId={} topic={} partition={} offset={}",
                                key, meta.topic(), meta.partition(), meta.offset());
                    }
                })
                .exceptionally(ex -> {
                    throw new OrderEventPublishException(
                            "Failed to publish to topic=" + topic + " orderId=" + key, ex);
                });
    }
}
