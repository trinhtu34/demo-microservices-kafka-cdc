package com.demo.orders.api;

import java.util.Map;

public record ApiError(String code, String message, Map<String, String> details) {
}
