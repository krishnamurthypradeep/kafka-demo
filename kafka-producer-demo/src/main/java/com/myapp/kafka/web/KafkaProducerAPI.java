package com.myapp.kafka.web;

import com.myapp.kafka.domain.Order;
import com.myapp.kafka.producer.OrderEventProducer;
import org.apache.coyote.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("api/v1/orders")
public class KafkaProducerAPI {

    private final OrderEventProducer orderEventProducer;

    public KafkaProducerAPI(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<String>> processOrders(@RequestBody List<Order> orders){


    return orderEventProducer.publishASynchronouslyWithBatch(orders)

            .thenApply(ignored -> ResponseEntity.ok("Orders acknowledged"));
    }
}
