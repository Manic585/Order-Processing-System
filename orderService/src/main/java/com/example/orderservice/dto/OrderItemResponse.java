package com.example.orderservice.dto;

import java.math.BigDecimal;

public record OrderItemResponse(
        String itemId,
        String productId,
        String productName,
        int quantity,
        BigDecimal unitPrice
) {}
