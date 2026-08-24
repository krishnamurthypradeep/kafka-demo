package com.myapp.kafka.consumer;

import com.myapp.kafka.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@Component
public class OrderEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final AtomicInteger invocation = new AtomicInteger();
    @KafkaListener(
    topics = "orders-topic",
    containerFactory = "processKafkaListenerFactory")
    public void processOrders(List<ConsumerRecord<Integer,Order>> orders){
        var count = invocation.incrementAndGet();
        System.out.printf("%s | invocation =%d | thread=%s | batchSize=%d%n",
                LocalDateTime.now(),count,Thread.currentThread().getName());

        orders.forEach(order ->
                logger.info("Order Details => key={}, partition={} offset={} order={} ",
                        order.key(),order.partition(),order.offset(),order.value()));
    }

}
