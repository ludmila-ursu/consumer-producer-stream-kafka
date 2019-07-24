package com.example.exercicekafka3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.exercicekafka3.consumer.Consumer;
import com.example.exercicekafka3.producer.Producer;

@SpringBootApplication
public class ExerciceKafka3Application {

	public static void main(String[] args) {
		SpringApplication.run(ExerciceKafka3Application.class, args);
		Producer producer = new Producer();
		producer.sendRecord();
		
		Consumer consumer = new Consumer();
		consumer.consumerSubscription();
		
	}

	
}
