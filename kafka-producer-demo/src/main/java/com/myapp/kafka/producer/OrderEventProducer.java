package com.myapp.kafka.producer;

import com.myapp.kafka.domain.Order;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
@Component
public class OrderEventProducer {

    private KafkaTemplate<Integer,String> kafkaTemplate;

    private ObjectMapper objectMapper;


    public OrderEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
       // this.objectMapper.registeredModules(new JavaTimeModule());
    }

    public void publishFireAndForget(List<Order> orders){
        orders.forEach(order ->{
        String orderJson =    objectMapper.writeValueAsString(order);
        kafkaTemplate.sendDefault(order.orderId(),orderJson);
        } );
    }


    public void publishSynchronously(List<Order> orders){
        orders.forEach(order ->{
            String orderJson =    objectMapper.writeValueAsString(order);
            try {
                CompletableFuture<SendResult<Integer,String>> future =
                kafkaTemplate.sendDefault(order.orderId(),orderJson);

             SendResult<Integer,String> result =   future.get(30, TimeUnit.SECONDS);
            RecordMetadata metadata = result.getRecordMetadata();
                System.out.printf("Order Sent Synchronously key=%d, partitions=%d,offset=%d%n",
                        order.orderId(),metadata.topic(),metadata.partition(),metadata.offset());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } );
    }

    public void publishASynchronously(List<Order> orders){
        orders.forEach(order ->{
            String orderJson =    objectMapper.writeValueAsString(order);
            try {
                CompletableFuture<SendResult<Integer,String>> future =
                        kafkaTemplate.sendDefault(order.orderId(),orderJson);

//                SendResult<Integer,String> result =   future.get(30, TimeUnit.SECONDS);

                future.orTimeout(30,TimeUnit.SECONDS).whenCompleteAsync((result,exception)->{
                    if(exception !=null){
                        System.out.printf("Order Sent Failed orderId=%d, errors=%s%n",
                                order.orderId(),exception.getMessage());
                        return;
                    }
                RecordMetadata metadata = result.getRecordMetadata();
                System.out.printf("Order Sent ASynchronously key=%d, partitions=%d,offset=%d%n",
                        order.orderId(),metadata.topic(),metadata.partition(),metadata.offset());
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } );
    }

    public CompletableFuture<Void> publishASynchronouslyWithBatch(List<Order> orders){

        List<CompletableFuture<?>> sends =
                orders.stream()
                        .map(order -> {
                            try {
                                String json = objectMapper.writeValueAsString(order);

                                long queuedAt = System.nanoTime();

                                return kafkaTemplate
                                        .sendDefault(order.orderId(), json)
                                        .whenComplete((result, exception) -> {
                                            long completedAt = System.nanoTime();

                                            double durationMs =
                                                    (completedAt - queuedAt)
                                                            / 1_000_000.0;

                                            if (exception != null) {
                                                System.err.printf(
                                                        "orderId=%d failed after %.2f ms: %s%n",
                                                        order.orderId(),
                                                        durationMs,
                                                        exception.getMessage());
                                                return;
                                            }

                                            System.out.printf(
                                                    "orderId=%d partition=%d " +
                                                            "offset=%d acknowledged after %.2f ms%n",
                                                    order.orderId(),
                                                    result.getRecordMetadata()
                                                            .partition(),
                                                    result.getRecordMetadata()
                                                            .offset(),
                                                    durationMs);
                                        });

                            } catch (Exception exception) {
                                return CompletableFuture.failedFuture(exception);
                            }
                        }).toList();

        return CompletableFuture.allOf(
                sends.toArray(CompletableFuture[]::new));
    }



}
