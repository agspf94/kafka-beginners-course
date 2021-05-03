package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        int max = 6;
        // Create a produce record and send data
        for (int i = 0; i < max; i++) {
            String topic = "first-topic";
            String value = Integer.toString(i);
            String key;
            if (i < max / 2)
                key = "id_" + 1;
            else
                key = "id_" + 2;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            int j = i;
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Key: " + key);
                    logger.info("Received new metadata (" + j + ") : \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing", exception);
                }
            });
        }

        // Flush data and close the producer
        producer.close();
    }
}
