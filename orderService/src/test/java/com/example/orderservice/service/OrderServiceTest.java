package com.example.orderservice.service;

import com.example.orderservice.cache.OrderCacheService;
import com.example.orderservice.domain.model.Order;
import com.example.orderservice.domain.model.OrderStatus;
import com.example.orderservice.domain.repository.OrderRepository;
import com.example.orderservice.dto.OrderItemRequest;
import com.example.orderservice.dto.OrderItemResponse;
import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.exception.InvalidOrderStateException;
import com.example.orderservice.kafka.event.OrderCreatedEvent;
import com.example.orderservice.kafka.exception.OrderEventPublishException;
import com.example.orderservice.kafka.producer.OrderEventProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderCacheService cacheService;

    @Mock
    private OrderEventProducer eventProducer;

    @InjectMocks
    private OrderService orderService;

    private static final String CUSTOMER_ID      = "customer-1";
    private static final String IDEMPOTENCY_KEY  = "idem-key-abc";
    private static final String PRODUCT_ID       = "PROD001";
    private static final BigDecimal UNIT_PRICE   = new BigDecimal("49.99");

    private OrderRequest request;

    @BeforeEach
    void setUp() {
        OrderItemRequest item = new OrderItemRequest(PRODUCT_ID, "Laptop", 1, UNIT_PRICE);
        request = new OrderRequest(CUSTOMER_ID, List.of(item), IDEMPOTENCY_KEY);
    }

    // ── placeOrder — success ──────────────────────────────────────────────────

    @Test
    @DisplayName("placeOrder: new order → saved, Kafka event published, cache populated")
    void testPlaceOrder_Success() {
        // Arrange — cache miss, no existing order in DB
        when(cacheService.getIdempotencyResult(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());
        when(orderRepository.findByIdempotencyKey(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());

        // Simulate DB assigning a UUID + lifecycle timestamps (normally done by @PrePersist)
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order o = invocation.getArgument(0);
            o.setId(UUID.randomUUID());
            o.setStatus(OrderStatus.PENDING);
            o.setCreatedAt(LocalDateTime.now());
            o.setUpdatedAt(LocalDateTime.now());
            return o;
        });

        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, Object>> okFuture =
                CompletableFuture.completedFuture(null);
        when(eventProducer.publishOrderCreated(any(OrderCreatedEvent.class))).thenReturn(okFuture);

        // Act
        OrderResponse response = orderService.placeOrder(request);

        // Assert — response shape
        assertThat(response).isNotNull();
        assertThat(response.customerId()).isEqualTo(CUSTOMER_ID);
        assertThat(response.status()).isEqualTo(OrderStatus.PENDING.name());
        assertThat(response.totalAmount()).isEqualByComparingTo(UNIT_PRICE);
        assertThat(response.message()).isEqualTo("Order placed successfully");

        // Assert — DB save called
        verify(orderRepository).save(any(Order.class));

        // Assert — Kafka event content via ArgumentCaptor
        ArgumentCaptor<OrderCreatedEvent> eventCaptor =
                ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(eventProducer).publishOrderCreated(eventCaptor.capture());
        OrderCreatedEvent publishedEvent = eventCaptor.getValue();
        assertThat(publishedEvent.customerId()).isEqualTo(CUSTOMER_ID);
        assertThat(publishedEvent.items()).hasSize(1);
        assertThat(publishedEvent.items().get(0).productId()).isEqualTo(PRODUCT_ID);
        assertThat(publishedEvent.totalAmount()).isEqualByComparingTo(UNIT_PRICE);

        // Assert — cache populated
        verify(cacheService).cacheOrder(anyString(), any(OrderResponse.class));
        verify(cacheService).cacheOrderStatus(anyString(), eq(OrderStatus.PENDING.name()));
        verify(cacheService).storeIdempotencyResult(eq(IDEMPOTENCY_KEY), any(OrderResponse.class));
    }

    // ── placeOrder — idempotent (Redis hit) ───────────────────────────────────

    @Test
    @DisplayName("placeOrder: duplicate idempotency key in Redis → return cached, no DB/Kafka")
    void testPlaceOrder_Idempotent() {
        // Arrange — cache already holds the result for this key
        OrderResponse cachedResponse = new OrderResponse(
                UUID.randomUUID().toString(), CUSTOMER_ID, OrderStatus.PENDING.name(),
                UNIT_PRICE, List.of(), LocalDateTime.now(), "Order already processed");
        when(cacheService.getIdempotencyResult(IDEMPOTENCY_KEY))
                .thenReturn(Optional.of(cachedResponse));

        // Act
        OrderResponse response = orderService.placeOrder(request);

        // Assert — returns the exact cached instance
        assertThat(response).isSameAs(cachedResponse);

        // Assert — no DB interaction, no Kafka publish
        verify(orderRepository, never()).findByIdempotencyKey(any());
        verify(orderRepository, never()).save(any());
        verify(eventProducer, never()).publishOrderCreated(any());
    }

    // ── placeOrder — Kafka failure compensation ───────────────────────────────

    @Test
    @DisplayName("placeOrder: Kafka publish fails → order status set to FAILED, exception propagated")
    void testPlaceOrder_KafkaFailure() {
        // Arrange — new order, Kafka will blow up
        when(cacheService.getIdempotencyResult(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());
        when(orderRepository.findByIdempotencyKey(IDEMPOTENCY_KEY)).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order o = invocation.getArgument(0);
            o.setId(UUID.randomUUID());
            o.setStatus(OrderStatus.PENDING);
            o.setCreatedAt(LocalDateTime.now());
            o.setUpdatedAt(LocalDateTime.now());
            return o;
        });

        // Returning a failed future causes .join() to throw CompletionException,
        // which is caught by the service's compensation block.
        CompletableFuture<SendResult<String, Object>> failedFuture =
                CompletableFuture.failedFuture(new RuntimeException("Broker unavailable"));
        when(eventProducer.publishOrderCreated(any())).thenReturn(failedFuture);

        // Act + Assert — exception surfaces as OrderEventPublishException
        assertThatThrownBy(() -> orderService.placeOrder(request))
                .isInstanceOf(OrderEventPublishException.class)
                .hasMessageContaining("Kafka publish failed");

        // Assert — save called twice: once for PENDING, once for FAILED compensation
        verify(orderRepository, times(2)).save(any(Order.class));

        // Assert — second save carries FAILED status
        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository, times(2)).save(orderCaptor.capture());
        Order compensationSave = orderCaptor.getAllValues().get(1);
        assertThat(compensationSave.getStatus()).isEqualTo(OrderStatus.FAILED);
    }

    // ── cancelOrder — success ─────────────────────────────────────────────────

    @Test
    @DisplayName("cancelOrder: PENDING order owned by caller → status=CANCELLED, cache evicted, event published")
    void testCancelOrder_Success() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        Order pendingOrder = Order.builder()
                .id(orderId)
                .customerId(CUSTOMER_ID)
                .status(OrderStatus.PENDING)
                .totalAmount(UNIT_PRICE)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
        when(eventProducer.publishOrderCancelled(any())).thenReturn(CompletableFuture.completedFuture(null));

        // Act
        orderService.cancelOrder(orderId.toString(), CUSTOMER_ID);

        // Assert — order persisted with CANCELLED status
        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(orderCaptor.capture());
        assertThat(orderCaptor.getValue().getStatus()).isEqualTo(OrderStatus.CANCELLED);

        // Assert — cache evicted
        verify(cacheService).evictOrder(orderId.toString());

        // Assert — cancellation event fired (fire-and-forget)
        verify(eventProducer).publishOrderCancelled(any(OrderCreatedEvent.class));
    }

    // ── cancelOrder — wrong customer ──────────────────────────────────────────

    @Test
    @DisplayName("cancelOrder: order belongs to different customer → InvalidOrderStateException")
    void testCancelOrder_WrongCustomer() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        Order order = Order.builder()
                .id(orderId)
                .customerId("actual-owner")
                .status(OrderStatus.PENDING)
                .totalAmount(UNIT_PRICE)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // Act + Assert
        // Note: the service throws InvalidOrderStateException for ownership violations;
        // there is no separate UnauthorizedException in this implementation.
        assertThatThrownBy(() -> orderService.cancelOrder(orderId.toString(), "wrong-customer"))
                .isInstanceOf(InvalidOrderStateException.class)
                .hasMessageContaining("does not belong to customer");

        verify(orderRepository, never()).save(any());
        verify(cacheService, never()).evictOrder(any());
        verify(eventProducer, never()).publishOrderCancelled(any());
    }

    // ── cancelOrder — already confirmed ──────────────────────────────────────

    @Test
    @DisplayName("cancelOrder: order already CONFIRMED → InvalidOrderStateException")
    void testCancelOrder_AlreadyConfirmed() {
        // Arrange
        UUID orderId = UUID.randomUUID();
        Order confirmedOrder = Order.builder()
                .id(orderId)
                .customerId(CUSTOMER_ID)
                .status(OrderStatus.CONFIRMED)
                .totalAmount(UNIT_PRICE)
                .idempotencyKey(IDEMPOTENCY_KEY)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(confirmedOrder));

        // Act + Assert
        assertThatThrownBy(() -> orderService.cancelOrder(orderId.toString(), CUSTOMER_ID))
                .isInstanceOf(InvalidOrderStateException.class)
                .hasMessageContaining("Only PENDING orders can be cancelled")
                .hasMessageContaining("CONFIRMED");

        verify(orderRepository, never()).save(any());
    }
}
