package org.apache.hadoop.hdfs.server.namenode;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.mortbay.log.Log;

import java.io.*;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;

/**
 * Created by gabriel on 2018-08-09.
 */
public class S3Util {

  private final AmazonS3 s3;

  public S3Util(AmazonS3 s3) {
    this.s3 = s3;
  }

  public File createLocalFile(String filename, int size) {
    Log.info("Creating file: " + filename + " with size: " + size);
    try {
      PrintWriter writer = new PrintWriter(filename, "UTF-8");
      for(int j=0; j < size; j++) {
        writer.write("0");
      }
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
      System.out.println("Success: uploaded file: " + file.getName() + " to bucket: " + bucketName);
      s3.putObject(request);
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


  public static AmazonS3Client setupS3(Configuration conf, String bucketPath) {
    Log.info("Setting up S3 environment");

    conf.set(DFS_DATANODE_DATA_DIR_KEY,
            "[DISK]//${hadoop.tmp.dir}/dfs/data,[PROVIDED]"+bucketPath);

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED, bucketPath); // this is the bucket to use

    // load access keys from local file
    final String credFileName =
            "/home/gabriel/Documents/hops/hadoop-tools/hadoop-fs2img/src/test/java/org/apache/hadoop/hdfs/server/namenode/awsCred.txt";
    // setup aws credentials
    S3Util.setSystemPropertiesS3Credentials(credFileName);
    Log.info("Loaded AWS credentials from file " + credFileName);

    AmazonS3Client s3 = new AmazonS3Client();
    s3.setRegion(Region.getRegion(Regions.EU_WEST_1));

    return s3;
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
