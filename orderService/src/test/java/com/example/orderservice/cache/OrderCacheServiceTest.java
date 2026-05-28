package com.example.orderservice.cache;

import com.example.orderservice.dto.OrderItemResponse;
import com.example.orderservice.dto.OrderResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderCacheServiceTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ValueOperations<String, Object> valueOps;

    @InjectMocks
    private OrderCacheService orderCacheService;

    private static final String ORDER_ID = UUID.randomUUID().toString();

    private OrderResponse sampleResponse;

    @BeforeEach
    void setUp() {
        // Wire the ValueOperations stub used by all read/write paths
        when(redisTemplate.opsForValue()).thenReturn(valueOps);

        sampleResponse = new OrderResponse(
                ORDER_ID,
                "customer-1",
                "PENDING",
                new BigDecimal("49.99"),
                List.of(new OrderItemResponse(UUID.randomUUID().toString(), "PROD001",
                        "Laptop", 1, new BigDecimal("49.99"))),
                LocalDateTime.now(),
                "Order placed successfully");
    }

    // ── getCachedOrder ────────────────────────────────────────────────────────

    @Test
    @DisplayName("getCachedOrder: Redis hit → returns populated Optional")
    void testGetCachedOrder_Hit() {
        when(valueOps.get("order:" + ORDER_ID)).thenReturn(sampleResponse);

        Optional<OrderResponse> result = orderCacheService.getCachedOrder(ORDER_ID);

        assertThat(result).isPresent();
        assertThat(result.get()).isSameAs(sampleResponse);
    }

    @Test
    @DisplayName("getCachedOrder: key absent in Redis → returns empty Optional")
    void testGetCachedOrder_Miss() {
        when(valueOps.get("order:" + ORDER_ID)).thenReturn(null);

        Optional<OrderResponse> result = orderCacheService.getCachedOrder(ORDER_ID);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("getCachedOrder: Redis throws → returns empty Optional without propagating exception")
    void testGetCachedOrder_RedisDown() {
        // Simulate a broken connection at the opsForValue() call level
        when(redisTemplate.opsForValue()).thenThrow(new RuntimeException("Redis connection refused"));

        assertThatNoException().isThrownBy(() -> {
            Optional<OrderResponse> result = orderCacheService.getCachedOrder(ORDER_ID);
            assertThat(result).isEmpty();
        });
    }

    // ── cacheOrder ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("cacheOrder: stores value under 'order:{id}' with 10-minute TTL")
    void testCacheOrder() {
        orderCacheService.cacheOrder(ORDER_ID, sampleResponse);

        verify(valueOps).set(
                eq("order:" + ORDER_ID),
                eq(sampleResponse),
                eq(Duration.ofMinutes(10)));
    }

    // ── evictOrder ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("evictOrder: deletes both 'order:{id}' and 'order:status:{id}' keys")
    void testEvictOrder() {
        orderCacheService.evictOrder(ORDER_ID);

        verify(redisTemplate).delete("order:" + ORDER_ID);
        verify(redisTemplate).delete("order:status:" + ORDER_ID);
        // ValueOperations should never be touched during eviction
        verify(redisTemplate, never()).opsForValue();
    }
}
