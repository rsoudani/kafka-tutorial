package com.rsoudani.tutorials.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "hello world");
        //send data - async
        producer.send(producerRecord);
        //flush data
        producer.flush();
        //close producer
        producer.close();

    }
}
