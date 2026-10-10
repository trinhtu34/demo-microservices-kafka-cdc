package com.demo.orders.application;

public class OrderIdempotencyConflictException extends RuntimeException {

    public OrderIdempotencyConflictException(String message) {
        super(message);
    }
}
