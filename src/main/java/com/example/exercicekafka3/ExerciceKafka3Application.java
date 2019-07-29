package com.example.exercicekafka3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.exercicekafka3.consumer.Consumer;
import com.example.exercicekafka3.producer.Producer;
import com.example.exercicekafka3.stream.Stream;

@SpringBootApplication
public class ExerciceKafka3Application {

	public static void main(String[] args) {
		SpringApplication.run(ExerciceKafka3Application.class, args);
		Producer producer = new Producer();
		producer.sendRecord();

		Consumer consumer = new Consumer();
		consumer.consumerSubscription();
		System.out.println("consumer end");
		
		Stream stream = new Stream();
		stream.kafkaStream();
	}

}
