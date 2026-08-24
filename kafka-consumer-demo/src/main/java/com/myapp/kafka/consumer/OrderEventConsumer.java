package com.myapp.kafka.consumer;

import com.myapp.kafka.domain.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class OrderEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(
    topics = "orders-topic",
    containerFactory = "processKafkaListenerFactory")
    public void processOrders(List<Order> orders){

        orders.forEach(order -> logger.info("Order Details => {}",order));
    }

}
