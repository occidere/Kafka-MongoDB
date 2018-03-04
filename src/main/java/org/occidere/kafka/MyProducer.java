package org.occidere.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {
	private final KafkaProducer<String, String> PRODUCER;
	
	private final String SERVERS;
	public MyProducer(String SERVERS) {
		this.SERVERS = SERVERS;
		this.PRODUCER = getKafkaProducer();
	}
	
	private KafkaProducer<String, String> getKafkaProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		return new KafkaProducer<>(props);
	}
	
	public void send(final String TOPIC, String msg) {
		PRODUCER.send(new ProducerRecord<>(TOPIC, msg), (metadata, exception) -> {
			if(metadata != null) {
				System.out.println("partitions(" + metadata.partition() + "), offset(" + metadata.offset() + "), " + msg);
			}
			else {
				exception.printStackTrace();
			}
		});
	}
	
	public void close() {
		PRODUCER.close();
	}
}
