package com.myapp.kafka.producer;

import com.myapp.kafka.domain.Order;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
public class OrderEventProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderEventProducer(
            KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<?> publishAsynchronouslyWithBatch(
            List<Order> orders) {

        List<CompletableFuture<?>> sends = orders.stream()
                .map(this::sendOrder).collect(Collectors.toUnmodifiableList());


        return CompletableFuture.allOf(
                sends.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<?> sendOrder(Order order) {

        /*
         * Capture the start time before invoking KafkaTemplate.send().
         */
        long startedAt = System.nanoTime();

        try {
            return kafkaTemplate
                    .sendDefault(
                            order.getOrderId().toString(),
                            order)
                    .whenComplete((result, exception) -> {

                        double durationMs =
                                elapsedMilliseconds(startedAt);

                        if (exception != null) {
                            logFailure(
                                    order,
                                    exception,
                                    durationMs);
                            return;
                        }

                        System.out.printf(
                                "ACKNOWLEDGED: orderId=%d, " +
                                        "topic=%s, partition=%d, " +
                                        "offset=%d, duration=%.2f ms%n",
                                Integer.valueOf(order.getOrderId().toString()),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                durationMs);
                    });

        } catch (Exception exception) {
            /*
             * Handles failures thrown before a future is returned,
             * such as an immediate serialization/configuration error.
             */
            double durationMs =
                    elapsedMilliseconds(startedAt);

            logFailure(order, exception, durationMs);

            return CompletableFuture.failedFuture(exception);
        }
    }

    private void logFailure(
            Order order,
            Throwable exception,
            double durationMs) {

        Throwable rootCause = getRootCause(exception);

        if (rootCause instanceof RetriableException) {
            /*
             * Kafka normally retries this type of error internally.
             * Receiving it here means retries or delivery timeout
             * were exhausted.
             */
            System.err.printf(
                    "RETRIES EXHAUSTED: orderId=%d, " +
                            "duration=%.2f ms, exception=%s, " +
                            "message=%s%n",
                    order.getOrderId(),
                    durationMs,
                    rootCause.getClass().getSimpleName(),
                    rootCause.getMessage());
        } else {
            System.err.printf(
                    "NON-RETRIABLE/FINAL FAILURE: orderId=%s, " +
                            "duration=%.2f ms, exception=%s, message=%s%n",
                    order.getOrderId(),
                    durationMs,
                    rootCause.getClass().getSimpleName(),
                    rootCause.getMessage());
        }
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

        kafkaTemplate.metrics().forEach((metricName, metric) -> {

            boolean requiredMetric =
                    metricName.name().equals("record-send-total")
                            || metricName.name()
                            .equals("record-retry-total")
                            || metricName.name()
                            .equals("record-error-total");

            if (requiredMetric) {
                System.out.printf(
                        "%s %s = %s%n",
                        metricName.name(),
                        metricName.tags(),
                        metric.metricValue());
            }
        });
    }

    public void printProducerMetrics(String topic) {

        kafkaTemplate.metrics().forEach((metricName, metric) -> {

            boolean requiredMetric =
                    metricName.name().equals("compression-rate-avg")
                            || metricName.name()
                            .equals("record-send-total")
                            || metricName.name()
                            .equals("record-retry-total")
                            || metricName.name()
                            .equals("request-size-avg")
                            || metricName.name()
                            .equals("request-size-max");

            if (!requiredMetric) {
                return;
            }

            String metricTopic =
                    metricName.tags().get("topic");

            /*
             * Aggregate metrics do not contain a topic tag.
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