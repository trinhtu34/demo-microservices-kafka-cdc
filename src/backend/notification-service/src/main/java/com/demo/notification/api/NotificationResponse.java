package com.demo.notification.api;

public record NotificationResponse(
        long id,
        String orderId,
        String sku,
        int quantity,
        String status,
        String reason,
        String createdAt) {
}
