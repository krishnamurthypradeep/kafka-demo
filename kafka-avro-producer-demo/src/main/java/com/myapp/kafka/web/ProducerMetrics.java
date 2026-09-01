package com.myapp.kafka.web;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("api/metrics")
public class ProducerMetrics {

    private final ProducerFactory<String, String> producerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    public ProducerMetrics(ProducerFactory<String, String> producerFactory, KafkaTemplate<String, String> kafkaTemplate) {
        this.producerFactory = producerFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    public Map<String,Object> getMetrics() {
       Map<String,Object> values = new LinkedHashMap<>();
       kafkaTemplate.metrics().forEach((metricName,metricValue) -> values.put(metricName.group()+"."+metricName.name(),metricValue.metricValue()));
    return values;
    }
}
// linger.ms =0
// linger.ms =20
// linger.ms =100
