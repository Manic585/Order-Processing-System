package com.example.inventoryservice.service;

import com.example.inventoryservice.domain.model.Product;
import com.example.inventoryservice.domain.repository.ProductRepository;
import com.example.inventoryservice.kafka.event.OrderItemEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    // ── Check and reserve ─────────────────────────────────────────────────────

    /**
     * For each item in the order:
     *   1. Fetch the product with an optimistic lock.
     *   2. Verify available_quantity >= requested quantity.
     *   3. Decrement available_quantity, increment reserved_quantity.
     *
     * All updates happen in a single transaction; if any product is
     * unavailable the whole transaction rolls back (no partial reservation).
     *
     * The @CircuitBreaker wraps the entire method. If the DB is degraded,
     * the fallback publishes an inventory.updated event with success=false
     * so the order-service can transition the order to FAILED without waiting.
     */
    @Transactional
    @CircuitBreaker(name = "inventory-db", fallbackMethod = "checkAndReserveFallback")
    public InventoryCheckResult checkAndReserveInventory(
            String orderId, List<OrderItemEvent> items) {

        log.info("event=INVENTORY_CHECK_START orderId={} itemCount={}", orderId, items.size());

        List<String> insufficientProducts = new ArrayList<>();

        for (OrderItemEvent item : items) {
            Product product = productRepository.findByIdWithLock(item.productId())
                    .orElse(null);

            if (product == null) {
                log.warn("event=PRODUCT_NOT_FOUND orderId={} productId={}",
                        orderId, item.productId());
                insufficientProducts.add(item.productId() + " (not found)");
                continue;
            }

            if (product.getAvailableQuantity() < item.quantity()) {
                log.warn("event=INSUFFICIENT_STOCK orderId={} productId={} available={} requested={}",
                        orderId, item.productId(), product.getAvailableQuantity(), item.quantity());
                insufficientProducts.add(item.productId() +
                        " (available=" + product.getAvailableQuantity() +
                        ", requested=" + item.quantity() + ")");
            }
        }

        if (!insufficientProducts.isEmpty()) {
            String reason = "Insufficient stock for: " + String.join(", ", insufficientProducts);
            log.warn("event=INVENTORY_CHECK_FAILED orderId={} reason={}", orderId, reason);
            return InventoryCheckResult.failure(reason);
        }

        // All products available — apply the reservations
        for (OrderItemEvent item : items) {
            Product product = productRepository.findByIdWithLock(item.productId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Product disappeared between check and reserve: " + item.productId()));

            product.setAvailableQuantity(product.getAvailableQuantity() - item.quantity());
            product.setReservedQuantity(product.getReservedQuantity() + item.quantity());
            productRepository.save(product);

            log.debug("event=STOCK_RESERVED productId={} qty={} newAvailable={}",
                    item.productId(), item.quantity(), product.getAvailableQuantity());
        }

        log.info("event=INVENTORY_CHECK_SUCCESS orderId={}", orderId);
        return InventoryCheckResult.ok();
    }

    /**
     * Circuit-breaker fallback for checkAndReserveInventory.
     * Called when the DB circuit is OPEN or a DB exception propagates after retries.
     */
    public InventoryCheckResult checkAndReserveFallback(
            String orderId, List<OrderItemEvent> items, Throwable ex) {

        log.error("event=INVENTORY_CB_OPEN orderId={} error={}", orderId, ex.getMessage());
        return InventoryCheckResult.failure("Service temporarily unavailable");
    }

    // ── Release reservation ───────────────────────────────────────────────────

    /**
     * Reverses a previously applied reservation (called on order cancellation).
     * Items whose products are not found are silently skipped with a warning.
     */
    @Transactional
    @CircuitBreaker(name = "inventory-db", fallbackMethod = "releaseReservationFallback")
    public void releaseReservation(String orderId, List<OrderItemEvent> items) {
        log.info("event=INVENTORY_RELEASE_START orderId={}", orderId);

        for (OrderItemEvent item : items) {
            productRepository.findByIdWithLock(item.productId()).ifPresentOrElse(product -> {
                int newReserved = Math.max(0, product.getReservedQuantity() - item.quantity());
                int released    = product.getReservedQuantity() - newReserved;

                product.setReservedQuantity(newReserved);
                product.setAvailableQuantity(product.getAvailableQuantity() + released);
                productRepository.save(product);

                log.debug("event=STOCK_RELEASED productId={} qty={} newAvailable={}",
                        item.productId(), released, product.getAvailableQuantity());

            }, () -> log.warn("event=RELEASE_PRODUCT_NOT_FOUND orderId={} productId={}",
                    orderId, item.productId()));
        }

        log.info("event=INVENTORY_RELEASE_COMPLETE orderId={}", orderId);
    }

    public void releaseReservationFallback(
            String orderId, List<OrderItemEvent> items, Throwable ex) {

        log.error("event=INVENTORY_RELEASE_CB_OPEN orderId={} error={}", orderId, ex.getMessage());
        // Best-effort: log and move on. The reservation will age out or be reconciled manually.
    }
}
