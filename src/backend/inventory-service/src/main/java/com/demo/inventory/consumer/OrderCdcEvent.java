package com.demo.inventory.consumer;

public record OrderCdcEvent(String orderId, String sku, int quantity) {
}
