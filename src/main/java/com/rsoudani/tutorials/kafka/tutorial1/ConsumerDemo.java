package com.rsoudani.tutorials.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


@Slf4j
public class ConsumerDemo {
    public static void main(String[] args) {
        String bootstrapServer = "localhost:9092";
        String groupId = "myJavaGroupId2";
        String topic = "first_topic";

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        Consumer consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to topics
        consumer.subscribe(Collections.singleton(topic));

        //pool for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                log.info(record.key() + " : " + record.value());
                log.info("Partition: " + record.partition());
                log.info("offset: " + record.offset());

            }
        }
    }
}
