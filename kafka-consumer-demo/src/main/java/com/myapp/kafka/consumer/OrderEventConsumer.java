package com.myapp.kafka.consumer;

import com.myapp.kafka.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.kafka.support.Acknowledgment;


@Component
public class OrderEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final AtomicInteger invocation = new AtomicInteger();
    @KafkaListener(
    topics = {"orders-topic","payments"},
    containerFactory = "processKafkaListenerFactory")
    public void processOrders(List<ConsumerRecord<Integer,Order>> orders,
                              Acknowledgment acknowledgment){
        var count = invocation.incrementAndGet();
        try {
            System.out.printf("%s | invocation =%d | thread=%s | batchSize=%d%n",
                    LocalDateTime.now(), count, Thread.currentThread().getName(), orders.size());

            for (int index = 0; index < orders.size(); index++) {

                logger.info("Order Details => {} ", orders.get(index).value());

//            orders.forEach(order ->
//                    logger.info("Order Details => key={}, partition={} offset={} order={} ",
//                            order.key(),order.partition(),order.offset(),order.value()));
            acknowledgment.acknowledge(index);
        }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
