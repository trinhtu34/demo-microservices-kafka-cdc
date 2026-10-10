package com.demo.inventory.consumer;

public record OrderCdcEvent(String orderId, String customerId, String sku, int quantity) {
}
