package com.myapp.kafka.consumer;

import com.myapp.kafka.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class OrderEventConsumer {

    private static final Logger logger =
            LoggerFactory.getLogger(OrderEventConsumer.class);

    private final AtomicInteger invocation =
            new AtomicInteger();

    @KafkaListener(
            topics = "orders-avro-topic",
            containerFactory = "processKafkaListenerFactory")
    public void processOrders(
            List<ConsumerRecord<String, Order>> records,
            Acknowledgment acknowledgment) {

        int count = invocation.incrementAndGet();

        logger.info(
                "{} | invocation={} | thread={} | batchSize={}",
                LocalDateTime.now(),
                count,
                Thread.currentThread().getName(),
                records.size());

        for (ConsumerRecord<String, Order> record : records) {
            processOrder(record);
        }

        /*
         * Commit offsets only after the complete batch succeeds.
         */
        acknowledgment.acknowledge();

        logger.info(
                "Batch invocation {} successfully acknowledged",
                count);
    }

    private void processOrder(
            ConsumerRecord<String, Order> record) {

        Order order = record.value();

        logger.info(
                "Order consumed: key={}, orderId={}, " +
                        "customerId={}, orderType={}, " +
                        "topic={}, partition={}, offset={}",
                record.key(),
                order.getOrderId(),
                order.getCustomerId(),
                order.getOrderType(),
                record.topic(),
                record.partition(),
                record.offset());

        order.getLineItems().forEach(lineItem ->
                logger.info(
                        "Line item: productId={}, productName={}, " +
                                "quantity={}, unitPrice={}",
                        lineItem.getProductId(),
                        lineItem.getProductName(),
                        lineItem.getQuantity(),
                        lineItem.getUnitPrice()));
    }
}