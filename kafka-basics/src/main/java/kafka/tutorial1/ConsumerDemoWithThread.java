package kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


@Slf4j
public class ConsumerDemoWithThread {
    public static void main(String[] args) {        //pool for new data
        ConsumerDemoWithThread consumer = new ConsumerDemoWithThread();
        consumer.doStaff();
    }
    public void doStaff(){
        String bootstrapServer = "localhost:9092";
        String groupId = "myJavaGroupId3";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic, latch);
        Thread myThread = new Thread(myConsumerRunnable);

        myThread.run();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("In shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("App interrupted", e);
        }finally {
            log.info("App is closing");
        }
    }
    public class ConsumerRunnable implements Runnable{

        public ConsumerRunnable() {
        }

        private CountDownLatch latch;
        private Consumer consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            //create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            consumer = new KafkaConsumer<>(properties);

            //subscribe consumer to topics
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info(record.key() + " : " + record.value());
                        log.info("Partition: " + record.partition());
                        log.info("offset: " + record.offset());

                    }
                }
            }catch (WakeupException e){
                log.info("received shutdown signal");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup();

        }
    }
}
