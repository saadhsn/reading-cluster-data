package com.kafka.consumer;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;


public abstract class MyKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MyKafkaConsumer.class);

    protected KafkaConsumer kafkaConsumer = null;
    protected String kafkaTopic = null;

    protected void kafkaConsumerSetup(String _kafkaTopic) {
        logger.warn("[MyKafkaConsumer] - Setting up Kafka Consumer");
        logger.warn("[MyKafkaConsumer] - Kafka Topic: " + _kafkaTopic);

        this.kafkaTopic = _kafkaTopic;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
        } catch (IOException e ) { e.printStackTrace(); }
    }

    protected abstract void KafkaConsumerImpl();

    protected void KafkaConsumerClose() {
        if (kafkaConsumer != null) { kafkaConsumer.close(); }
    }

    public void run() { KafkaConsumerImpl(); }

    public void stop() { KafkaConsumerClose(); }
}
