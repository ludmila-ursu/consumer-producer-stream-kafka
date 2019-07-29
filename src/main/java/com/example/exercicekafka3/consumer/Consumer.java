package com.example.exercicekafka3.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.example.exercicekafka3.model.DataFile;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;




public class Consumer {

	int i = 0;
	public void consumerSubscription() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupIdExample");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		try (KafkaConsumer<String, DataFile> consumer = new KafkaConsumer<>(props)) {
			System.out.println("dans le try de consummer");

			consumer.subscribe(Arrays.asList("my_avro_topic"));
			System.out.println("après le subscribe du consommateur");

			
			}
		}
	}

