package com.example.exercicekafka3.stream;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import com.example.exercicekafka3.model.DataFile;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class Stream {

	public void kafkaStream() {
		System.out.println("init streams");
		Properties streamProperties = new Properties();
		streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkaStreamExample");
		streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
		streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		streamProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

		StreamsBuilder builder = new StreamsBuilder();
		// construct the KStream from an input topic -> my_avro_topic:
		KStream<Integer, DataFile> donneesRestauration = builder.stream("my_avro_topic");

		//send the KStream to a new topic -> output_topic:
		donneesRestauration.to("output_topic");

		//filter the data by a certain key (facility_city): 
		System.out.println("send KStream filtered_values to a new topic");
		donneesRestauration.filter((i, d) -> {
			return d.getFacilityCity().toString().equals("LOS ANGELES");
		}).to("filtered_values_topic");
		
		//print to the console the filtered result:
		System.out.println("send KStream filtered_values to a new topic");
		donneesRestauration.filter((i, d) -> {
			return d.getFacilityCity().toString().equals("LOS ANGELES");
		}).print(Printed.toSysOut());
		

		KafkaStreams streams = new KafkaStreams(builder.build(), streamProperties);
		streams.start();
		System.out.println("streams démarés");

	}
}
