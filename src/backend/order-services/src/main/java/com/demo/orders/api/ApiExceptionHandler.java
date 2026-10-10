package com.demo.orders.api;

import com.demo.orders.application.OrderIdempotencyConflictException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException exception) {
        Map<String, String> details = new LinkedHashMap<>();
        exception.getBindingResult().getFieldErrors().forEach(error ->
                details.putIfAbsent(error.getField(), error.getDefaultMessage()));

        return ResponseEntity.badRequest().body(
                new ApiError("VALIDATION_ERROR", "Order request is invalid", details));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadableRequest() {
        return ResponseEntity.badRequest().body(
                new ApiError("INVALID_REQUEST", "Request body must be valid order JSON", Map.of()));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ApiError> handleMissingHeader(MissingRequestHeaderException exception) {
        return ResponseEntity.badRequest().body(new ApiError(
                "MISSING_HEADER",
                "Idempotency-Key header is required",
                Map.of(exception.getHeaderName(), "is required")));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> handleInvalidHeader(MethodArgumentTypeMismatchException exception) {
        return ResponseEntity.badRequest().body(new ApiError(
                "INVALID_HEADER",
                "Idempotency-Key must be a UUID",
                Map.of(exception.getName(), "must be a UUID")));
    }

    @ExceptionHandler(OrderIdempotencyConflictException.class)
    public ResponseEntity<ApiError> handleIdempotencyConflict(OrderIdempotencyConflictException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(
                new ApiError("IDEMPOTENCY_KEY_CONFLICT", exception.getMessage(), Map.of()));
    }
}
