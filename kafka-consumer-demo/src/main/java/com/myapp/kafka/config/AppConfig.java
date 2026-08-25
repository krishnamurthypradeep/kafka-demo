package com.myapp.kafka.config;

import com.myapp.kafka.domain.Order;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Map;

@Configuration
public class AppConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<Integer, Order> processKafkaListenerFactory(
            ConsumerFactory<Integer,Order> consumerFactory, RebalanceListener rebalanceListener){

       var factory = new ConcurrentKafkaListenerContainerFactory<Integer,Order>();
       factory.setConsumerFactory(consumerFactory);
       factory.setBatchListener(true);
      // factory.getContainerProperties().setAckMode();
       factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
       factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
       return factory;
    }
}
