package com.myapp.kafka.web;

public record OrderLineItemRequest(
        String productId,
        String productName,
        int quantity,
        double unitPrice
) {
}
