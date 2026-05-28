package com.example.inventoryservice.service;

/**
 * Value object returned by ProductService.checkAndReserveInventory().
 *
 * Using a dedicated record instead of a plain boolean lets the circuit-breaker
 * fallback communicate a reason ("Service temporarily unavailable") back to the
 * caller without throwing an exception.
 */
public record InventoryCheckResult(boolean success, String failureReason) {

    public static InventoryCheckResult ok() {
        return new InventoryCheckResult(true, null);
    }

    public static InventoryCheckResult failure(String reason) {
        return new InventoryCheckResult(false, reason);
    }
}
