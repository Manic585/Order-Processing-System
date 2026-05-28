package com.example.orderservice.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.math.BigDecimal;

public record OrderItemRequest(
        @NotBlank(message = "productId must not be blank")
        String productId,

        @NotBlank(message = "productName must not be blank")
        String productName,

        @Min(value = 1, message = "quantity must be at least 1")
        int quantity,

        @DecimalMin(value = "0.01", message = "unitPrice must be at least 0.01")
        BigDecimal unitPrice
) {}
