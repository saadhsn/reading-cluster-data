package com.kafka.producer;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class MyKafkaProducer {
  private static final Logger logger = LoggerFactory.getLogger(MyKafkaProducer.class);
  protected KafkaProducer kafkaProducer = null;
  protected String kafkaTopic = null;

  protected void kafkaProducerSetup(String _kafkaTopic) {
    logger.warn("[MyKafkaProducer] - Setting up Kafka Producer");
    logger.warn("[MyKafkaProducer] - Kafka Topic: " + _kafkaTopic);

    this.kafkaTopic = _kafkaTopic;
      try (InputStream props = Resources.getResource("producer.props").openStream()) {
        Properties properties = new Properties();
        properties.load(props);
        kafkaProducer = new KafkaProducer<String, String>(properties);
      } catch (IOException e ) { e.printStackTrace(); }
  }

  protected abstract void kafkaProducerImpl();

  protected void kafkaProducerClose() {
    logger.warn("[MyKafkaProducer] - Closing Kafka Producer");
    if (kafkaProducer != null) { kafkaProducer.close(); }
  }

  public void run() { kafkaProducerImpl(); }
  public void stop() { kafkaProducerClose(); }
}
