package com.demo.notification.api;

import java.time.Instant;

public record FlowProgressEvent(
        String customerId,
        String orderId,
        String milestone,
        String topic,
        int partition,
        long offset,
        Instant observedAt) {
}
