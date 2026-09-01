package com.myapp.kafka.web;

import java.util.List;

public record OrderRequest(
        String orderId,
        String customerId,
        String orderType,
        long orderTime,
        double totalAmount,
        List<OrderLineItemRequest> lineItems
) {
}
