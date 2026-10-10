package com.demo.orders.persistence;

public record ExistingOrder(String customerId, String sku, int quantity) {

    public boolean matches(String requestedCustomerId, String requestedSku, int requestedQuantity) {
        return customerId.equals(requestedCustomerId)
                && sku.equals(requestedSku)
                && quantity == requestedQuantity;
    }
}
