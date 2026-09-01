package com.myapp.kafka.web;

import com.myapp.kafka.domain.Order;
import com.myapp.kafka.domain.OrderLineItem;
import com.myapp.kafka.domain.OrderType;
import com.myapp.kafka.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("api/v1/orders")
public class KafkaProducerAPI {

    private final OrderEventProducer orderEventProducer;

    public KafkaProducerAPI(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<String>> processOrders(
            @RequestBody List<OrderRequest> requests) {

        List<Order> orders = requests.stream()
                .map(this::toOrder)
                .toList();

        return orderEventProducer
                .publishAsynchronouslyWithBatch(orders)
                .thenApply(ignored ->
                        ResponseEntity.ok(
                                orders.size()
                                        + " order(s) acknowledged"));
    }

    private Order toOrder(OrderRequest request) {

        List<OrderLineItem> lineItems =
                request.lineItems().stream()
                        .map(this::toOrderLineItem)
                        .toList();

        return Order.newBuilder()
                .setOrderId(request.orderId())
                .setCustomerId(request.customerId())
                .setOrderType(
                        OrderType.valueOf(
                                request.orderType().toUpperCase()))
                .setOrderTime(request.orderTime())
                .setTotalAmount(request.totalAmount())
                .setLineItems(lineItems)
                .build();
    }

    private OrderLineItem toOrderLineItem(
            OrderLineItemRequest item) {

        return OrderLineItem.newBuilder()
                .setProductId(item.productId())
                .setProductName(item.productName())
                .setQuantity(item.quantity())
                .setUnitPrice(item.unitPrice())
                .build();
    }



    }
