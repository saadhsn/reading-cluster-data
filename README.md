# reading-cluster-data
This repository contains two programs _**gcloud-essentials** and  **kafka-with-gcloud**_ .  
* **gcloud-essentials** unzips the google cluster trace data and also concatenates the files of same category into one file  
* **kafka-with-gcloud** reads cluster trace data from google cloud storage and sends this data through [Apche Kafka] (http://kafka.apache.org/) for streaming application   

### running gcloud-essentials  
- `cd gcloud-essentials`  
- compiling code `mvn clean install` or `mvn package`  
- `cd target` this directory contains two jar files
- only run jar file with dependencies in this case *GCloudEssentials-1.0-SNAPSHOT-jar-with-dependencies.jar*  
- running jar file `java -cp pathToJar/GCloudEssentials-1.0-SNAPSHOT-jar-with-dependencies.jar: com.googleClusterTrace.Main`  

### running kafka-with-gcloud
- `cd kafka-with-gcloud`
- compiling code `mvn clean install` or `mvn package`  
- `cd target` this directory contains two jar files  
- only run jar file with dependencies in this case *KafkaWithGCloud-1.0-SNAPSHOT-jar-with-dependencies.jar*  
- running jar file `java -cp pathToJar/ KafkaWithGCloud-1.0-SNAPSHOT-jar-with-dependencies.jar: com.kafka.Main`    




