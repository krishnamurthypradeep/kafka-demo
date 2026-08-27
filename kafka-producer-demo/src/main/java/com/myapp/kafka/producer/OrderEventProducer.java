package com.myapp.kafka.producer;

import com.myapp.kafka.domain.Order;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.springframework.core.NestedExceptionUtils.getRootCause;

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
                                            long startedAt = System.nanoTime();
                                            double durationMs =
                                                    (completedAt - queuedAt)
                                                            / 1_000_000.0;

                                            if (exception != null) {
                                                Throwable rootCause = getRootCause(exception);
                                                double durationMS = elapsedMilliseconds(startedAt);


                                                if (rootCause instanceof RetriableException) {
                                                    /*
                                                     * The failure was retriable, but Kafka could not complete
                                                     * the send within retries/delivery.timeout.ms.
                                                     */
                                                    System.err.printf(
                                                            "%s RETRIES EXHAUSTED: orderId=%d, " +
                                                                    "duration=%.2f ms, exception=%s, message=%s%n",
                                                            "BATCH",
                                                            order.orderId(),
                                                            durationMs,
                                                            rootCause.getClass().getSimpleName(),
                                                            rootCause.getMessage());
                                                } else {
                                                    /*
                                                     * Examples include serialization errors,
                                                     * invalid topic names and oversized messages.
                                                     */
                                                    System.err.printf(
                                                            "%s NON-RETRIABLE/FINAL FAILURE: orderId=%d, " +
                                                                    "duration=%.2f ms, exception=%s, message=%s%n",
                                                            "BATCH",
                                                            order.orderId(),
                                                            durationMs,
                                                            rootCause.getClass().getSimpleName(),
                                                            rootCause.getMessage());
                                                }
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

    private Throwable getRootCause(Throwable exception) {
        Throwable cause = exception;

        while (cause.getCause() != null) {
            cause = cause.getCause();
        }

        return cause;
    }

    private double elapsedMilliseconds(long startedAt) {
        return (System.nanoTime() - startedAt) / 1_000_000.0;
    }

    public CompletableFuture<Void> testIdempotence(){
        List<CompletableFuture<SendResult<Integer, String>>> sends =
                IntStream.rangeClosed(1, 10000)
                        .mapToObj(orderId -> {
                            String message = """
                                {
                                  "orderId": %d,
                                  "message": "Order "+
                                }
                                """.formatted(orderId);

                            return kafkaTemplate
                                    .sendDefault(orderId, message)
                                    .whenComplete((result, exception) -> {
                                        if (exception != null) {
                                            System.err.printf(
                                                    "orderId=%d failed: %s%n",
                                                    orderId,
                                                    exception.getMessage());

                                            return;
                                        }

                                        System.out.printf(
                                                "orderId=%d partition=%d offset=%d%n",
                                                orderId,
                                                result.getRecordMetadata().partition(),
                                                result.getRecordMetadata().offset());
                                    });
                        })
                        .toList();

        return CompletableFuture.allOf(
                sends.toArray(CompletableFuture[]::new))
                .whenComplete((result, exception) -> {  printIdempotenceMetrics();});
    }

    public void printIdempotenceMetrics() {
        kafkaTemplate.metrics().forEach((name, metric) -> {
            if (name.name().equals("record-send-total")
                    || name.name().equals("record-retry-total")
                    || name.name().equals("record-error-total")) {

                System.out.printf(
                        "%s = %s%n",
                        name.name(),
                        metric.metricValue());
            }
        });
    }

    private void printProducerMetrics(String topic) {

        kafkaTemplate.metrics().forEach((metricName, metric) -> {

            boolean requiredMetric =
                    metricName.name().equals("compression-rate-avg")
                            || metricName.name().equals("record-send-total")
                            || metricName.name().equals("record-retry-total")
                            || metricName.name().equals("request-size-avg")
                            || metricName.name().equals("request-size-max");

            if (!requiredMetric) {
                return;
            }

            String metricTopic =
                    metricName.tags().get("topic");

            /*
             * Some producer metrics have a topic tag, while aggregate
             * producer metrics do not.
             */
            if (metricTopic == null || metricTopic.equals(topic)) {
                System.out.printf(
                        "%s %s = %s%n",
                        metricName.name(),
                        metricName.tags(),
                        metric.metricValue());
            }
        });
    }

    public CompletableFuture<Void> testInFlightAndCompression(){
        String payload = "ORDER-DESCRIPTION-".repeat(500);
        List<CompletableFuture<SendResult<Integer, String>>> sends =
                IntStream.rangeClosed(1, 10_000)
                        .mapToObj(orderId -> {
                            String message = """
                                {
                                  "sequenceId": %d,
                                  "orderId": %d,
                                  "description": "%s"
                                }
                                """.formatted(orderId,orderId,payload);

                            return kafkaTemplate
                                    .sendDefault(orderId, message)
                                    .whenComplete((result, exception) -> {
                                        if (exception != null) {
                                            System.err.printf(
                                                    "orderId=%d failed: %s%n",
                                                    orderId,
                                                    exception.getMessage());

                                            return;
                                        }

                                        System.out.printf(
                                                "orderId=%d partition=%d offset=%d%n",
                                                orderId,
                                                result.getRecordMetadata().partition(),
                                                result.getRecordMetadata().offset());
                                    });
                        })
                        .toList();

        return CompletableFuture.allOf(
                        sends.toArray(CompletableFuture[]::new))
                .whenComplete((result, exception) -> {  printIdempotenceMetrics();});
    }


}
