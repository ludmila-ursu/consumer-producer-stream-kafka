package com.example.exercicekafka3.producer;

import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.example.exercicekafka3.model.SimpleCard;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class Producer {

	public void sendRecord() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// configure serializers classes:
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
		// configure schema repository server
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
		// create the Avro objets for key and value:
		SimpleCard card = new SimpleCard("spades", "ace");
		
		// create the producer expecting avro schema:
		KafkaProducer<String, SimpleCard> avroProducer = new KafkaProducer<>(props);
	

		// create the avro record with the objects created earlier and send them:
		IntStream.range(1, 10).forEach(i -> {
			ProducerRecord<String, SimpleCard> record = new ProducerRecord<>("my_avro_topic", " " + i, card);
			avroProducer.send(record);
		});

		avroProducer.close();
	}
}
