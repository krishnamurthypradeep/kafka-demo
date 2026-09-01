package com.myapp.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;



@SpringBootApplication
public class KafkaProducerDemoApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaProducerDemoApplication.class, args);
	}



}


// We Have 3 ways in which messages can be sent by producers
// FireandForget
// Synchronous
// Asynchronous
