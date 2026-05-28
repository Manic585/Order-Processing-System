package com.example.orderservice.cache;

import com.example.orderservice.dto.OrderResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;

/**
 * Cache-aside helper for order data.
 *
 * Every operation is wrapped in a try-catch: if Redis is unavailable the
 * exception is logged and the caller receives an empty Optional (reads) or
 * a silent no-op (writes/evictions).  The request is never failed because
 * of a cache error.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderCacheService {

    private static final String ORDER_KEY_PREFIX       = "order:";
    private static final String STATUS_KEY_PREFIX      = "order:status:";
    private static final String IDEMPOTENCY_KEY_PREFIX = "idempotency:";

    private static final Duration ORDER_TTL       = Duration.ofMinutes(10);
    private static final Duration STATUS_TTL      = Duration.ofMinutes(5);
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    private final RedisTemplate<String, Object> redisTemplate;

    // ── Order ─────────────────────────────────────────────────────────────────

    public void cacheOrder(String orderId, OrderResponse order) {
        String key = ORDER_KEY_PREFIX + orderId;
        try {
            redisTemplate.opsForValue().set(key, order, ORDER_TTL);
            log.debug("event=CACHE_SET orderId={} key={}", orderId, key);
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=cacheOrder error={}", e.getMessage());
        }
    }

    public Optional<OrderResponse> getCachedOrder(String orderId) {
        String key = ORDER_KEY_PREFIX + orderId;
        try {
            Object value = redisTemplate.opsForValue().get(key);
            if (value instanceof OrderResponse cached) {
                log.info("event=CACHE_HIT orderId={}", orderId);
                return Optional.of(cached);
            }
            log.warn("event=CACHE_MISS orderId={}", orderId);
            return Optional.empty();
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=getCachedOrder error={}", e.getMessage());
            return Optional.empty();
        }
    }

    // ── Order status ──────────────────────────────────────────────────────────

    public void cacheOrderStatus(String orderId, String status) {
        String key = STATUS_KEY_PREFIX + orderId;
        try {
            redisTemplate.opsForValue().set(key, status, STATUS_TTL);
            log.debug("event=CACHE_SET_STATUS orderId={} status={}", orderId, status);
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=cacheOrderStatus error={}", e.getMessage());
        }
    }

    public Optional<String> getOrderStatus(String orderId) {
        String key = STATUS_KEY_PREFIX + orderId;
        try {
            Object value = redisTemplate.opsForValue().get(key);
            if (value instanceof String status) {
                log.info("event=CACHE_HIT orderId={}", orderId);
                return Optional.of(status);
            }
            log.warn("event=CACHE_MISS orderId={}", orderId);
            return Optional.empty();
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=getOrderStatus error={}", e.getMessage());
            return Optional.empty();
        }
    }

    // ── Eviction ──────────────────────────────────────────────────────────────

    /**
     * Evicts both the order cache entry and its status entry.
     * Silently continues if Redis is unavailable.
     */
    public void evictOrder(String orderId) {
        try {
            redisTemplate.delete(ORDER_KEY_PREFIX  + orderId);
            redisTemplate.delete(STATUS_KEY_PREFIX + orderId);
            log.debug("event=CACHE_EVICT orderId={}", orderId);
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=evictOrder error={}", e.getMessage());
        }
    }

    // ── Idempotency ───────────────────────────────────────────────────────────

    public void storeIdempotencyResult(String key, OrderResponse response) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + key;
        try {
            redisTemplate.opsForValue().set(redisKey, response, IDEMPOTENCY_TTL);
            log.debug("event=IDEMPOTENCY_STORED key={}", key);
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=storeIdempotencyResult error={}", e.getMessage());
        }
    }

    public Optional<OrderResponse> getIdempotencyResult(String key) {
        String redisKey = IDEMPOTENCY_KEY_PREFIX + key;
        try {
            Object value = redisTemplate.opsForValue().get(redisKey);
            if (value instanceof OrderResponse cached) {
                log.info("event=CACHE_HIT orderId={}", key);
                return Optional.of(cached);
            }
            log.warn("event=CACHE_MISS orderId={}", key);
            return Optional.empty();
        } catch (Exception e) {
            log.error("event=CACHE_ERROR operation=getIdempotencyResult error={}", e.getMessage());
            return Optional.empty();
        }
    }
}
