package com.example.orderservice.controller;

import com.example.orderservice.dto.OrderRequest;
import com.example.orderservice.dto.OrderResponse;
import com.example.orderservice.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    // ── POST /api/orders ──────────────────────────────────────────────────────

    /**
     * Places a new order.
     *
     * The Idempotency-Key header takes precedence over any idempotencyKey value
     * in the request body.  It is merged into a new OrderRequest record before
     * the service call so that the service layer is unaware of transport concerns.
     */
    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody OrderRequest request,
            @AuthenticationPrincipal UserDetails principal) {

        log.info("event=ORDER_REQUEST method=POST path=/api/orders customerId={}",
                principal.getUsername());

        // Merge header key into the immutable record
        OrderRequest requestWithKey = new OrderRequest(
                request.customerId(),
                request.items(),
                idempotencyKey);

        OrderResponse response = orderService.placeOrder(requestWithKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    // ── GET /api/orders/{orderId} ─────────────────────────────────────────────

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(
            @PathVariable String orderId,
            @AuthenticationPrincipal UserDetails principal) {

        log.info("event=ORDER_REQUEST method=GET path=/api/orders/{} customerId={}",
                orderId, principal.getUsername());

        return ResponseEntity.ok(orderService.getOrder(orderId));
    }

    // ── GET /api/orders/{orderId}/status ──────────────────────────────────────

    @GetMapping("/{orderId}/status")
    public ResponseEntity<String> getOrderStatus(
            @PathVariable String orderId,
            @AuthenticationPrincipal UserDetails principal) {

        log.info("event=ORDER_REQUEST method=GET path=/api/orders/{}/status customerId={}",
                orderId, principal.getUsername());

        return ResponseEntity.ok(orderService.getOrderStatus(orderId));
    }

    // ── GET /api/orders/customer/{customerId} ─────────────────────────────────

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<OrderResponse>> getOrdersByCustomer(
            @PathVariable String customerId,
            @AuthenticationPrincipal UserDetails principal) {

        log.info("event=ORDER_REQUEST method=GET path=/api/orders/customer/{} customerId={}",
                customerId, principal.getUsername());

        return ResponseEntity.ok(orderService.getOrdersByCustomer(customerId));
    }

    // ── DELETE /api/orders/{orderId} ──────────────────────────────────────────

    @DeleteMapping("/{orderId}")
    public ResponseEntity<Void> cancelOrder(
            @PathVariable String orderId,
            @RequestHeader("X-Customer-Id") String customerId,
            @AuthenticationPrincipal UserDetails principal) {

        log.info("event=ORDER_REQUEST method=DELETE path=/api/orders/{} customerId={}",
                orderId, principal.getUsername());

        orderService.cancelOrder(orderId, customerId);
        return ResponseEntity.noContent().build();
    }
}
