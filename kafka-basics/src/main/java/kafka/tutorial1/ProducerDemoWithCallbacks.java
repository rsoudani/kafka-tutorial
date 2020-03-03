package kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";

        log.info("test");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 1000; i++) {
            //create producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("first_topic", "hello world " + i);
            //send data - async
            producer.send(producerRecord, new Callback() {
                public void onCompletion(final RecordMetadata recordMetadata, final Exception e) {
                    //execute every time on success or error
                    if (e == null) {
                        //success
                        log.info("Received metadaa: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n"

                        );
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }
        //flush data
        producer.flush();
        //close producer
        producer.close();

    }
}
