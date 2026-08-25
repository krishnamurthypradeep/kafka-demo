package com.myapp.kafka;

import com.myapp.kafka.domain.Order;
import com.myapp.kafka.domain.OrderLineItem;
import com.myapp.kafka.domain.OrderType;
import com.myapp.kafka.producer.OrderEventProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@SpringBootApplication
public class KafkaProducerDemoApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaProducerDemoApplication.class, args);
	}

	//@Bean
	CommandLineRunner runner(OrderEventProducer producer){
		return args -> {
			List<Order> list=List.of(new Order(1002,"B12200",
					new BigDecimal(95456.5), OrderType.RESTAURANT,
					List.of(new OrderLineItem("Burger",3,new BigDecimal(1456.5))) ,LocalDateTime.now()));

			producer.publishASynchronously(list);
		};
	}

}


// We Have 3 ways in which messages can be sent by producers
// FireandForget
// Synchronous
// Asynchronous
