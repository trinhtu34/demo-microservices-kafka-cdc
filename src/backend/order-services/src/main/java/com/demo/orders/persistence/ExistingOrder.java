package com.demo.orders.persistence;

public record ExistingOrder(String sku, int quantity) {

    public boolean matches(String requestedSku, int requestedQuantity) {
        return sku.equals(requestedSku) && quantity == requestedQuantity;
    }
}
