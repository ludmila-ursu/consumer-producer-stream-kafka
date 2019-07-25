package com.example.exercicekafka3.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.example.exercicekafka3.model.SimpleCard;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;




public class Consumer {

	public void consumerSubscription() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupIdExample");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		
		try (KafkaConsumer<String, SimpleCard> consumer = new KafkaConsumer<>(props)) {

			consumer.subscribe(Arrays.asList("my_avro_topic"));

			while (true) {
				ConsumerRecords<String, SimpleCard> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, SimpleCard> record : records) {
					System.out.println("offset: " + record.offset() + ", record values:" + record.value());
					consumer.seekToBeginning(consumer.assignment());					
				}
			}
			
		}
	}
}	
