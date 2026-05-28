package com.example.orderservice.dto;

public record AuthResponse(
        String token,
        long expiresIn
) {}
