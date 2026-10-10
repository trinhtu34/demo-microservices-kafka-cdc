package com.demo.notification.consumer;

public record ReservationCdcEvent(String orderId, String sku, int quantity, String status, String reason) {

    public boolean isFinal() {
        return "RESERVED".equals(status) || "REJECTED".equals(status);
    }
}
