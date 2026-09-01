package com.myapp.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class KafkaConsumerDemoApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaConsumerDemoApplication.class, args);
	}



}


// We Have 3 ways in which messages can be sent by producers
// FireandForget
// Synchronous
// Asynchronous
