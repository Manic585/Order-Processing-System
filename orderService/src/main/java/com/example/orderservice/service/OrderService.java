package com.example.orderservice.service;

import com.example.orderservice.cache.OrderCacheService;
import com.example.orderservice.domain.model.Order;
import com.example.orderservice.domain.model.OrderItem;
import com.example.orderservice.domain.model.OrderStatus;
import com.example.orderservice.domain.repository.OrderRepository;
import com.example.orderservice.dto.OrderItemRequest;
import com.example.orderservice.dto.OrderItemResponse;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.exception.InvalidOrderStateException;
import com.example.orderservice.exception.OrderAlreadyExistsException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.kafka.event.OrderCreatedEvent;
import com.example.orderservice.kafka.event.OrderItemEvent;
import com.example.orderservice.kafka.exception.OrderEventPublishException;
import com.example.orderservice.kafka.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository     orderRepository;
    private final OrderCacheService   cacheService;
    private final OrderEventProducer  eventProducer;

    // ── Place order ───────────────────────────────────────────────────────────

    /**
     * Creates a new order with PENDING status, publishes an OrderCreatedEvent,
     * and caches the result.
     *
     * Idempotency: duplicate requests carrying the same idempotencyKey return
     * the original response without re-creating the order.
     *
     * Kafka failure compensation: if the publish fails after DB commit, the
     * order status is updated to FAILED and the exception is re-thrown.
     * noRollbackFor ensures that FAILED status is committed rather than
     * rolled back alongside the original PENDING save.
     *
     * Redis note: with enableTransactionSupport=true on the RedisTemplate,
     * Redis reads inside a @Transactional method are queued in MULTI mode
     * and always return null.  The Redis idempotency check is therefore
     * effectively a no-op here — the DB check is the authoritative fallback.
     * Redis writes (caching at the end) are queued and flushed via EXEC after
     * the DB transaction commits, so the cache is populated for subsequent reads.
     */
    @Transactional(noRollbackFor = OrderEventPublishException.class)
    public OrderResponse placeOrder(OrderRequest request) {
        log.info("event=ORDER_PLACE_START customerId={} idempotencyKey={}",
                request.customerId(), request.idempotencyKey());

        // ── 1. Redis idempotency check (best-effort; see note above) ──────────
        Optional<OrderResponse> cached =
                cacheService.getIdempotencyResult(request.idempotencyKey());
        if (cached.isPresent()) {
            log.info("event=IDEMPOTENT_RETURN_CACHE idempotencyKey={}", request.idempotencyKey());
            return cached.get();
        }

        // ── 2. DB idempotency fallback ────────────────────────────────────────
        Optional<Order> existing =
                orderRepository.findByIdempotencyKey(request.idempotencyKey());
        if (existing.isPresent()) {
            log.info("event=IDEMPOTENT_RETURN_DB idempotencyKey={}", request.idempotencyKey());
            OrderResponse response = toOrderResponse(existing.get(), "Order already processed");
            cacheService.storeIdempotencyResult(request.idempotencyKey(), response);
            return response;
        }

        // ── 3. Build and persist the order ────────────────────────────────────
        BigDecimal totalAmount = request.items().stream()
                .map(i -> i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        Order order = Order.builder()
                .customerId(request.customerId())
                .status(OrderStatus.PENDING)
                .totalAmount(totalAmount)
                .idempotencyKey(request.idempotencyKey())
                .build();

        List<OrderItem> orderItems = request.items().stream()
                .map(i -> toOrderItemEntity(i, order))
                .toList();
        order.getItems().addAll(orderItems);

        Order savedOrder;
        try {
            savedOrder = orderRepository.save(order);
        } catch (DataIntegrityViolationException e) {
            // Race condition: another thread just inserted the same idempotency key.
            throw new OrderAlreadyExistsException(
                    "Order with idempotencyKey already exists: " + request.idempotencyKey());
        }

        log.info("event=ORDER_SAVED orderId={} customerId={}",
                savedOrder.getId(), savedOrder.getCustomerId());

        // ── 4. Publish to Kafka ───────────────────────────────────────────────
        // .join() blocks until broker acknowledges, giving a definitive result
        // before we decide whether to keep PENDING or flip to FAILED.
        OrderCreatedEvent event = toOrderCreatedEvent(savedOrder);
        try {
            eventProducer.publishOrderCreated(event).join();
        } catch (CompletionException | OrderEventPublishException e) {
            log.error("event=ORDER_KAFKA_FAILED orderId={} error={}",
                    savedOrder.getId(), e.getMessage());
            savedOrder.setStatus(OrderStatus.FAILED);
            orderRepository.save(savedOrder);
            // noRollbackFor lets this FAILED update commit; re-throw for the caller.
            throw new OrderEventPublishException(
                    "Kafka publish failed for orderId=" + savedOrder.getId(), e);
        }

        // ── 5. Cache results ──────────────────────────────────────────────────
        OrderResponse response = toOrderResponse(savedOrder, "Order placed successfully");
        String orderIdStr = savedOrder.getId().toString();
        cacheService.cacheOrder(orderIdStr, response);
        cacheService.cacheOrderStatus(orderIdStr, OrderStatus.PENDING.name());
        cacheService.storeIdempotencyResult(request.idempotencyKey(), response);

        log.info("event=ORDER_PLACED orderId={} customerId={} totalAmount={}",
                savedOrder.getId(), savedOrder.getCustomerId(), savedOrder.getTotalAmount());
        return response;
    }

    // ── Get order ─────────────────────────────────────────────────────────────

    /**
     * Not annotated @Transactional: items are loaded via JOIN FETCH in
     * findByIdWithItems(), and Redis reads work correctly outside a transaction
     * because MULTI mode is not entered without an active Spring transaction.
     */
    public OrderResponse getOrder(String orderId) {
        Optional<OrderResponse> cached = cacheService.getCachedOrder(orderId);
        if (cached.isPresent()) {
            return cached.get();
        }

        Order order = orderRepository.findByIdWithItems(UUID.fromString(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

        OrderResponse response = toOrderResponse(order, "Order retrieved");
        cacheService.cacheOrder(orderId, response);

        log.info("event=ORDER_FETCHED orderId={}", orderId);
        return response;
    }

    // ── Get order status ──────────────────────────────────────────────────────

    public String getOrderStatus(String orderId) {
        Optional<String> cached = cacheService.getOrderStatus(orderId);
        if (cached.isPresent()) {
            return cached.get();
        }

        Order order = orderRepository.findById(UUID.fromString(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

        String status = order.getStatus().name();
        cacheService.cacheOrderStatus(orderId, status);

        log.info("event=ORDER_STATUS_FETCHED orderId={} status={}", orderId, status);
        return status;
    }

    // ── Get orders by customer ────────────────────────────────────────────────

    public List<OrderResponse> getOrdersByCustomer(String customerId) {
        log.info("event=ORDERS_BY_CUSTOMER_FETCH customerId={}", customerId);

        List<Order> orders = orderRepository.findByCustomerIdWithItems(customerId);
        return orders.stream()
                .map(o -> toOrderResponse(o, "Order retrieved"))
                .toList();
    }

    // ── Cancel order ──────────────────────────────────────────────────────────

    @Transactional
    public OrderResponse cancelOrder(String orderId, String customerId) {
        log.info("event=ORDER_CANCEL_START orderId={} customerId={}", orderId, customerId);

        Order order = orderRepository.findById(UUID.fromString(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

        if (!order.getCustomerId().equals(customerId)) {
            log.warn("event=AUTH_FAILURE username={} reason=Order does not belong to customer",
                    customerId);
            throw new InvalidOrderStateException(
                    "Order " + orderId + " does not belong to customer " + customerId);
        }

        if (order.getStatus() != OrderStatus.PENDING) {
            throw new InvalidOrderStateException(
                    "Only PENDING orders can be cancelled. Current status: " + order.getStatus());
        }

        order.setStatus(OrderStatus.CANCELLED);
        Order savedOrder = orderRepository.save(order);

        cacheService.evictOrder(orderId);

        // Fire-and-forget: cancellation events are best-effort (no blocking join)
        eventProducer.publishOrderCancelled(toOrderCreatedEvent(savedOrder));

        log.info("event=ORDER_CANCELLED orderId={} customerId={}", orderId, customerId);
        return toOrderResponse(savedOrder, "Order cancelled");
    }

    // ── Update order status (called by Kafka consumer) ────────────────────────

    @Transactional
    public void updateOrderStatus(String orderId, OrderStatus newStatus) {
        log.info("event=ORDER_STATUS_UPDATE orderId={} newStatus={}", orderId, newStatus);

        Order order = orderRepository.findById(UUID.fromString(orderId))
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

        OrderStatus previous = order.getStatus();
        order.setStatus(newStatus);
        orderRepository.save(order);

        // Evict the full-order cache so the next read fetches fresh data.
        // Update the lightweight status cache immediately.
        cacheService.evictOrder(orderId);
        cacheService.cacheOrderStatus(orderId, newStatus.name());

        log.info("event=ORDER_STATUS_UPDATED orderId={} from={} to={}",
                orderId, previous, newStatus);
    }

    // ── Mapping helpers ───────────────────────────────────────────────────────

    private OrderResponse toOrderResponse(Order order, String message) {
        List<OrderItemResponse> items = order.getItems().stream()
                .map(i -> new OrderItemResponse(
                        i.getId().toString(),
                        i.getProductId(),
                        i.getProductName(),
                        i.getQuantity(),
                        i.getUnitPrice()))
                .toList();

        return new OrderResponse(
                order.getId().toString(),
                order.getCustomerId(),
                order.getStatus().name(),
                order.getTotalAmount(),
                items,
                order.getCreatedAt(),
                message);
    }

    private OrderItem toOrderItemEntity(OrderItemRequest req, Order order) {
        return OrderItem.builder()
                .order(order)
                .productId(req.productId())
                .productName(req.productName())
                .quantity(req.quantity())
                .unitPrice(req.unitPrice())
                .build();
    }

    private OrderCreatedEvent toOrderCreatedEvent(Order order) {
        List<OrderItemEvent> eventItems = order.getItems().stream()
                .map(i -> new OrderItemEvent(
                        i.getProductId(),
                        i.getProductName(),
                        i.getQuantity(),
                        i.getUnitPrice()))
                .toList();

        return new OrderCreatedEvent(
                order.getId().toString(),
                order.getCustomerId(),
                eventItems,
                order.getTotalAmount(),
                Instant.now());
    }
}
