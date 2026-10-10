package com.demo.orders.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank(message = "must not be blank")
        @Pattern(regexp = "USER_A|USER_B", message = "must be USER_A or USER_B")
        String customerId,

        @NotBlank(message = "must not be blank")
        @Size(max = 50, message = "must be at most 50 characters")
        String sku,

        @NotNull(message = "is required")
        @Positive(message = "must be greater than 0")
        Integer quantity
) {
}
