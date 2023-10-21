package com.gathi.demos.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.gathi.demos.kafka.common.KafkaConstants.CONSUMER_GP_JAVA_APPLICATION;
import static com.gathi.demos.kafka.common.KafkaConstants.MAIN_TOPIC;

public class ConsumerDemo {

    Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {

        ConsumerDemo cd = new ConsumerDemo();
        cd.consume();
    }

    public void consume() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", CONSUMER_GP_JAVA_APPLICATION);
        properties.setProperty("auto.offset.reset", "earliest");
        //above property can be earliest, latest, none
        //earliest - last offset commit point
        //latest - always latest
        //none - Throw an exception if no offset present in the consumer group

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    log.info("Detecting a shutdown hook, calling consumer.wakeup to close the consumer");
                    consumer.wakeup();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
        );

        try {
            consumer.subscribe(List.of(MAIN_TOPIC));
            while (true) {

                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> rec : records) {
                    log.info("key: " + rec.key() + " | value:" + rec.value());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer wake up is triggered", e);
        } catch (Exception e) {
            log.info("Unknown exception is triggered", e);
        } finally {
            log.info("Shutting down consumer");
            consumer.close();
        }
    }
}
