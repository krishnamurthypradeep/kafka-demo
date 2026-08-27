package com.myapp.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AppConfig {

    @Value("${app.kafka.topic.orders}")
    private String topicName;

    @Value("${app.kafka.topic.partitions}")
    private int partitions;

    @Value("${app.kafka.topic.replicas}")
    private int replicas;

    // kafka-topics --create --topic products-prices --partitions 3 --replication-factor --bootstrap-server

    @Bean
    NewTopic orderTopic(){
        return TopicBuilder.name(topicName).partitions(partitions).replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,"2").build();
    }

//    @Bean
//    NewTopic paymentsTopic(){
//        return TopicBuilder.name("payments").partitions(3).replicas(3).build();
//    }

}
