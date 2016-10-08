package com.kafka.producer;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.Iterator;
/**
 * Created by Nouman
 */
public class KafkaProducerForGCloud extends MyKafkaProducer {
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerForGCloud.class);

  private Storage storageObject = null;
  private Bucket sourceBucketObject = null;
  private String sourceBucketName = null;
  private String bucketDirectory = null;

  private void gcloudSetup(String _sourceBucketName, String _bucketDirectory) {
    logger.warn("[KafkaProducerForGCloud] - Setting up GCloud");
    logger.warn("[JobEventsPKafkaProducerForGCloudroducer] - Bucket Name: " + _sourceBucketName);
    logger.warn("[KafkaProducerForGCloud] - Bucket Directory: " + _bucketDirectory);

    this.sourceBucketName = _sourceBucketName;
    this.bucketDirectory = _bucketDirectory;
    storageObject = StorageOptions.defaultInstance().service();
    sourceBucketObject = storageObject.get(sourceBucketName);

  }

  protected void kafkaProducerImpl() {
    logger.warn("[KafkaProducerForGCloud] - Reading records from GCS");

    ByteArrayInputStream byteArrayInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;
    //ProducerRecord sends to data to any conusmer source, Spark, KafkaConcumer etc
    ProducerRecord kafkaProducerRecord = null;

    Iterator<Blob> blobIterator = sourceBucketObject.list().iterateAll();
      while(blobIterator.hasNext()) {
        Blob currentBlob = blobIterator.next();
        if (currentBlob.name().contains(bucketDirectory)) {
          logger.info("[KafkaProducerForGCloud] - Currently Reading - " + currentBlob.name());

          byteArrayInputStream = new ByteArrayInputStream(currentBlob.content());
          inputStreamReader = new InputStreamReader(byteArrayInputStream);
          bufferedReader = new BufferedReader(inputStreamReader);
          String line = null;
          try {
            while((line = bufferedReader.readLine()) != null) {
              kafkaProducerRecord = new ProducerRecord<String, String>(kafkaTopic, line);
              kafkaProducer.send(kafkaProducerRecord);
            }
          } catch (Exception e) { e.printStackTrace(); }

        } else { continue; }
      }
  }

  public KafkaProducerForGCloud(String _kafkaTopic, String _sourceBucketName, String _bucketDirectory) {
    gcloudSetup(_sourceBucketName, _bucketDirectory);
    kafkaProducerSetup(_kafkaTopic);
  }

}
