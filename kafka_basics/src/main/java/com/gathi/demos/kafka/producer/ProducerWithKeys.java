package com.gathi.demos.kafka.producer;

import com.gathi.demos.kafka.common.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class.getName());

    public static void main(String[] args) {

        ProducerWithKeys producer = new ProducerWithKeys();
        producer.produce();
    }

    public void produce() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<3; i++) {
            for (int j=0; j<10; j++) {

                String key = "_id_" + i+j;
                String value = "Message " + i+j;

                ProducerRecord<String, String> record1 = new ProducerRecord<>(
                        KafkaConstants.MAIN_TOPIC, key, value);

                //async op
                producer.send(record1, (recordMetadata, e) -> {
                    logger.info("key: " + key + " | partition: " + recordMetadata.partition());
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //close it so that it will flush remaining messages before termination
        producer.close();
    }
}
