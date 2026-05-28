package com.example.orderservice.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record OrderRequest(
        @NotBlank(message = "customerId must not be blank")
        String customerId,

        @NotEmpty(message = "items must not be empty")
        @Valid
        List<OrderItemRequest> items,

        String idempotencyKey
) {}
