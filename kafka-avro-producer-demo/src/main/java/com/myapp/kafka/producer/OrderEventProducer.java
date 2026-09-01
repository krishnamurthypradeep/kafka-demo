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

//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
@Component
public class OrderEventProducer {

    private KafkaTemplate<String,Order> kafkaTemplate;

    private ObjectMapper objectMapper;


    public OrderEventProducer(KafkaTemplate<String, Order> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
       // this.objectMapper.registeredModules(new JavaTimeModule());
    }






    public CompletableFuture<Void> publishASynchronouslyWithBatch(List<Order> orders){

        List<CompletableFuture<?>> sends =
                orders.stream()
                        .map(order -> {
                            try {


                                long queuedAt = System.nanoTime();

                                return kafkaTemplate
                                        .sendDefault(order.getOrderId().toString(), order)
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
                                                            order.getOrderId(),
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
                                                            order.getOrderId(),
                                                            durationMs,
                                                            rootCause.getClass().getSimpleName(),
                                                            rootCause.getMessage());
                                                }
                                                return;
                                            }

                                            System.out.printf(
                                                    "orderId=%d partition=%d " +
                                                            "offset=%d acknowledged after %.2f ms%n",
                                                    order.getOrderId(),
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




}
