package com.googleClusterTrace;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;


public class ConcatJobEvents {

  protected Storage storageObject = null;
  protected Bucket sourceBucketObject = null;
  protected Bucket destinationBucketObject = null;
  protected Bucket destinationBucketObject_2 = null;
  protected String sourceBucketName = "clusterdata-2011-2";
  protected String destinationBucketName = "clusterdata-unzipped";
  protected String destinationBucketName_2 = "clusterdata-unzipped-single";
  protected String directory = "job_events";

  public ConcatJobEvents() {
    storageObject = StorageOptions.defaultInstance().service();
    sourceBucketObject = storageObject.get(sourceBucketName);
    destinationBucketObject = getStorageBucket(destinationBucketName);
    destinationBucketObject_2 = getStorageBucket(destinationBucketName_2);
  }

  protected Bucket getStorageBucket(String bucketName) {
    if (storageObject.get(bucketName) == null) {
      return storageObject.create(BucketInfo.of(bucketName));
    }
    else {
      return storageObject.get(bucketName);
    }
  }

  protected void unzipBlobs() {
    Iterator<Blob> blobIterator = sourceBucketObject.list().iterateAll();
    List<Blob> listOfBlobs = new ArrayList<>();

    while(blobIterator.hasNext()) {
      Blob currentBlob = blobIterator.next();
      if (currentBlob.name().contains(directory)) {
        listOfBlobs.add(currentBlob);
      }
    }

    byte[] buffer = new byte[1024];

    try {
      BlobId bigBlobId = BlobId.of(destinationBucketName_2, "job_events.csv");
      BlobInfo bigBlobInfo = BlobInfo.builder(bigBlobId).contentType("text/csv").build();
      Blob bigBlob = storageObject.create(bigBlobInfo);
      WriteChannel bigBlobWriter = bigBlob.writer();

      for (Blob currentBlob: listOfBlobs) {
        String currentBlobName = currentBlob.name().replace(".gz", "");
        BlobId blobId = BlobId.of(destinationBucketName, currentBlobName);
        BlobInfo blobInfo = BlobInfo.builder(blobId).contentType("text/csv").build();
        Blob newBlob = storageObject.create(blobInfo);
        GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(currentBlob.content()));
        WriteChannel blobWriter = newBlob.writer();
        int len;
        while ((len = gzis.read(buffer)) > 0) {
          blobWriter.write(ByteBuffer.wrap(buffer, 0 ,len));
          bigBlobWriter.write(ByteBuffer.wrap(buffer, 0 ,len));
        }
        blobWriter.close();
        System.out.println(currentBlobName + " created successfully!");
      }
      bigBlobWriter.close();
    } catch (IOException e) { e.printStackTrace(); }
  }


  public void run() {
  unzipBlobs();
  }
  
}
