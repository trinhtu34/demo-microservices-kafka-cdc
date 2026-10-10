package com.demo.orders.api;

import com.demo.orders.application.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<OrderCreatedResponse> createOrder(
            @RequestHeader("Idempotency-Key") java.util.UUID idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request) {
        var result = orderService.createOrder(
                idempotencyKey.toString(), request.customerId(), request.sku().strip(), request.quantity());
        HttpStatus status = result.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(new OrderCreatedResponse(result.orderId()));
    }
}
