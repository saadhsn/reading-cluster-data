package com.kafka;

import com.kafka.producer.KafkaProducerForGCloud;



public class Main {
    public static void main(String[] args) {
        KafkaProducerForGCloud kpfg = new KafkaProducerForGCloud("job_events", "clusterdata-unzipped", "job_events");
        kpfg.run();
        kpfg.stop();
    }
}
