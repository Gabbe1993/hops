package org.apache.hadoop.hdfs.server.namenode;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by gabriel on 2018-08-09.
 */
public class S3Util {

  private final AmazonS3 s3;

  public S3Util(AmazonS3 s3) {
    this.s3 = s3;
  }

  public File createFile(String filename) {
    try {
      PrintWriter writer = new PrintWriter(filename, "UTF-8");
      writer.println("The first line");
      writer.println("The second line");
      writer.close();
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return new File(filename);
  }

  public void uploadFile(String bucketName, String fileObjKeyName, File file) {
    try {
      // Upload a file as a new object with ContentType and title specified.
      PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, file);
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentType("plain/text");
      metadata.addUserMetadata("x-amz-meta-title", "someTitle");
      metadata.setContentMD5(null);
      request.setMetadata(metadata);
      s3.putObject(request);
      System.out.println("Success: uploaded file: " + file.getName() + " to bucket: " + bucketName);
    } catch (Exception e) {
      System.err.println("Failed: Could not upload: " + file.getName() + " to bucket: " + bucketName);
      e.printStackTrace();
    }
  }

  private Bucket getBucket(String bucket_name) {
    Bucket named_bucket = null;
    List<Bucket> buckets = s3.listBuckets();
    for (Bucket b : buckets) {
      if (b.getName().equals(bucket_name)) {
        named_bucket = b;
      }
    }
    return named_bucket;
  }

  public Bucket createBucket(String bucket_name) {
    Bucket b = null;
    if (s3.doesBucketExist(bucket_name)) {
      System.out.format("Bucket %s already exists.\n", bucket_name);
      b = getBucket(bucket_name);
    } else {
      try {
        b = s3.createBucket(bucket_name);
      } catch (AmazonS3Exception e) {
        System.err.println(e);
      }
    }
    return b;
  }

  public static void setSystemPropertiesS3Credentials(String credFileName) {
   try {
      // set up new properties object from file
      FileInputStream propFile =
              new FileInputStream(credFileName);
      Properties p =
              new Properties(System.getProperties());
      p.load(propFile);

      // set the system properties
      System.setProperties(p);
    } catch (IOException e) {
      System.err.println("Could not load aws credentials from " + credFileName);
      e.printStackTrace();
    }
  }
}
