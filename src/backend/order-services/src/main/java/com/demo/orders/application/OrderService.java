package com.demo.orders.application;

import com.demo.orders.persistence.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public OrderCreationResult createOrder(String requestId, String customerId, String sku, int quantity) {
        int inserted = orderRepository.insertIfAbsent(requestId, customerId, sku, quantity);
        if (inserted == 1) {
            return new OrderCreationResult(requestId, true);
        }

        var existingOrder = orderRepository.findById(requestId)
                .orElseThrow(() -> new IllegalStateException("Order idempotency record was not found"));
        if (!existingOrder.matches(customerId, sku, quantity)) {
            throw new OrderIdempotencyConflictException(
                    "Idempotency-Key was already used with a different order payload");
        }
        return new OrderCreationResult(requestId, false);
    }
}
