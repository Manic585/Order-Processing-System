package com.example.orderservice.exception.handler;

import com.example.orderservice.dto.ErrorResponse;
import com.example.orderservice.exception.InvalidOrderStateException;
import com.example.orderservice.exception.OrderAlreadyExistsException;
import com.example.orderservice.exception.OrderNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // ── 404 ───────────────────────────────────────────────────────────────────

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrderNotFound(
            OrderNotFoundException e, HttpServletRequest request) {

        log.warn("event=ORDER_NOT_FOUND path={} message={}", request.getRequestURI(), e.getMessage());
        return build(HttpStatus.NOT_FOUND, "Order Not Found", e.getMessage(), request);
    }

    // ── 409 ───────────────────────────────────────────────────────────────────

    @ExceptionHandler(OrderAlreadyExistsException.class)
    public ResponseEntity<ErrorResponse> handleOrderAlreadyExists(
            OrderAlreadyExistsException e, HttpServletRequest request) {

        log.warn("event=ORDER_ALREADY_EXISTS path={} message={}", request.getRequestURI(), e.getMessage());
        return build(HttpStatus.CONFLICT, "Order Already Exists", e.getMessage(), request);
    }

    // ── 422 ───────────────────────────────────────────────────────────────────

    @ExceptionHandler(InvalidOrderStateException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOrderState(
            InvalidOrderStateException e, HttpServletRequest request) {

        log.warn("event=INVALID_ORDER_STATE path={} message={}", request.getRequestURI(), e.getMessage());
        return build(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid Order State", e.getMessage(), request);
    }

    // ── 400 ───────────────────────────────────────────────────────────────────

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(
            ConstraintViolationException e, HttpServletRequest request) {

        String message = e.getConstraintViolations().stream()
                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                .collect(Collectors.joining("; "));

        log.warn("event=VALIDATION_FAILED path={} message={}", request.getRequestURI(), message);
        return build(HttpStatus.BAD_REQUEST, "Validation Failed", message, request);
    }

    /** Handles @Valid failures on @RequestBody arguments. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValid(
            MethodArgumentNotValidException e, HttpServletRequest request) {

        String message = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));

        log.warn("event=VALIDATION_FAILED path={} message={}", request.getRequestURI(), message);
        return build(HttpStatus.BAD_REQUEST, "Validation Failed", message, request);
    }

    // ── 500 ───────────────────────────────────────────────────────────────────

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(
            Exception e, HttpServletRequest request) {

        log.error("event=INTERNAL_ERROR path={} error={}", request.getRequestURI(), e.getMessage(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error",
                "An unexpected error occurred", request);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private ResponseEntity<ErrorResponse> build(
            HttpStatus status, String error, String message, HttpServletRequest request) {

        return ResponseEntity.status(status).body(new ErrorResponse(
                Instant.now(),
                status.value(),
                error,
                message,
                request.getRequestURI()));
    }
}
