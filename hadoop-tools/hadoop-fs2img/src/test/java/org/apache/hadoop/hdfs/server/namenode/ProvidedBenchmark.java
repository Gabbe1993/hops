package org.apache.hadoop.hdfs.server.namenode;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Created by gabriel on 2018-08-08.
 */

public class ProvidedBenchmark {

  @Rule
  public TestName name = new TestName();
  public static final Logger LOG =
          LoggerFactory.getLogger(ProvidedBenchmark.class);

  private final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
  private final Path pBASE = new Path(fBASE.toURI().toString());
  private final Path nnDirPath = new Path(pBASE, "nnDir"); // folder for .csv file
  private Configuration conf;
  private MiniDFSCluster cluster;
  private ITestProvidedImplementation test;
  private AmazonS3 s3;
  private S3Util s3Util;
  private final String BUCKET_NAME = "benchmark-provided-2"; //
  private final String BUCKET_PATH = "s3a://" + BUCKET_NAME + "/";
  private String RESULTS_DIR = "/home/gabriel/Documents/hops/benchmarks";

  public ProvidedBenchmark() {
    try {
      setupConfig();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Before
  public void setupConfig() throws Exception {
    test = new ITestProvidedImplementation();
    conf = test.getConf();

    s3 = S3Util.setupS3(conf, BUCKET_PATH);
    s3Util = new S3Util(s3);

    //s3Util.createBucket(BUCKET_NAME);
  }

  @After
  public void shutdown() throws Exception {
    try {
      if (cluster != null) {
        cluster.shutdown();
      }
    } finally {
      cluster = null;
    }
  }

  @Test
  public void stripFile() {
    String fileName = "/home/gabriel/Documents/hops/benchmarks/KB_benchmarkRead-Mon Aug 20 14:46:58 CEST 2018.txt";
    List<String> list = new ArrayList<>();
    try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
      list = stream
              .filter(line -> line.contains("took"))
              .map(line -> line.split("took ")[1]
                      //.replace(" ms", "")
              )
              .filter(line -> Integer.parseInt(line) < 3000)
              .collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
    list.forEach(System.out::println);

  }

  // 2 .except to see no difference in time regarding file size
  @Test
  public void benchmarkFiles() throws Exception {
    int files = 100;
    int mb = 1048576;
    int size = mb;
    int runsPerTest = 10;
    String resultFile = "benchmarkFiles-" + new Date() + ".txt";
    BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile, true));

    String[] filenames = new String[files + 1];
    for (int i = 1; i < files + 1; i++) {
      size *= 2;
      String filename;
      filename = "test-file-" + i + ".txt";
      File file = s3Util.createLocalFile(filename, size);
      s3Util.uploadFile(BUCKET_NAME, filename, file);
      filenames[i] = filename;

      scanBucket(3); // warmup

      long totalRunTime = 0L;
      LOG.info("-------------------------start test " + i + " ---------------------------------");
      for (int j = 0; j < runsPerTest; j++) {
        LOG.info("Benchmarking scan of " + BUCKET_NAME + " with files " + Arrays.toString(filenames));
        long startTime = System.currentTimeMillis();
        scanBucket(1);
        long endTime = System.currentTimeMillis() - startTime;
        LOG.info("Test took: " + endTime + " ms");
        totalRunTime += endTime;
      }
      String result = i + " = " + totalRunTime / runsPerTest + "  ms";
      saveResults(writer, result);
      LOG.info("-------------------------saved results to " + resultFile + "----------------------------------");

      s3.deleteObject(BUCKET_NAME, filename);
      Thread.sleep(3000); // sleep to make sure it is deleted
    }

    writer.close();
  }

  public void saveResults(BufferedWriter writer, String result) throws IOException {
    writer.append(result);
    writer.append('\n');
    writer.flush();
  }
  @Test
  public void calculateAvg() {
    String fileName = "/home/gabriel/Documents/hops/benchmarks/SMALL-KB_benchmarkRead-Tue Aug 21 11:09:44 CEST 2018.txt";
    List<String> avgs = new ArrayList<>();
    List<String> list = new ArrayList<>();
    try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
      list = stream
              .filter(line -> line.contains("took") && !line.contains("reading"))
              .map(line -> line.split("took ")[1]
                      //.replace(" ms", "")
              )
              //.filter(a -> Integer.parseInt(a) < 1000)
              .collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
    }
    list.forEach(System.out::println);
  }

  @Test
  public void benchmarkRead() throws Exception {
    int replication = 4;
    int limit = 0; // remove outliers over this ms

    // GABRIEL - change before tests of small/med/big
    String type = "BIG-MB_";
    int baseFileLen = 1024 * 1024 * 100; // 1 kb * 1 mb * x

    RESULTS_DIR = RESULTS_DIR + "/read";
    String resultFile = type + "benchmarkRead-" + new Date() + ".txt";
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(RESULTS_DIR, resultFile), true));

    int tries = 10;
    int start = 1; // number to start from
    String remoteFilename;
    String baseDir = "/";

    File file;
    DFSClient client;
    String diskFilename;
    int fileLen;
    try {
      for (int i = start; i < tries+1; i++) {
        remoteFilename = "remoteFile_" + i + ".txt";
        fileLen = baseFileLen * i;
        file = s3Util.createLocalFile(remoteFilename, fileLen);
    s3Util.uploadFile(BUCKET_NAME, remoteFilename, file);
    file.delete();
        Thread.sleep(3000);

        test.createImage(new FSTreeWalk(new Path(BUCKET_PATH), conf),
                nnDirPath, FixedBlockResolver.class);

        cluster = test.startCluster(nnDirPath, 2,
                null,
                new StorageType[][]{
                        {StorageType.PROVIDED, StorageType.DISK},
                        {StorageType.DISK}
                },
                false, null);
        client = new DFSClient(new InetSocketAddress("localhost",
                cluster.getNameNodePort()), cluster.getConfiguration(0));

        diskFilename = "/local_" + i + ".txt";
        test.createFile(new Path(diskFilename), (short) 1, fileLen, fileLen);
        Thread.sleep(3000);

        // warmup
        timeReadFromFile(client, cluster.getFileSystem(), diskFilename, fileLen, 0);
        timeReadFromFile(client, cluster.getFileSystem(), baseDir + remoteFilename, fileLen, 0);

        writer.write(
                timeReadFromFile(client, cluster.getFileSystem(), diskFilename, fileLen, limit)
        );
        writer.write(
                timeReadFromFile(client, cluster.getFileSystem(), baseDir + remoteFilename, fileLen, limit)
        );
        writer.flush();

        cluster.shutdown();
        System.gc();
        Thread.sleep(3000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      writer.close();
      cluster.shutdown();
    }
  }

  private String timeReadFromFile(DFSClient client, DistributedFileSystem fileSystem, String filename, int length, int limit) throws IOException {
    Path path = new Path(filename);
    Assert.assertTrue(filename + " does not exist in filesystem " + fileSystem.toString(), fileSystem.exists(path));

    FSDataInputStream in = fileSystem.open(path);
    StringBuilder sb = new StringBuilder();
    LocatedBlocks lbks = client.getLocatedBlocks(filename, 0, filename.length());
    DatanodeInfo[] infos = lbks.get(0).getLocations();
    sb.append("reading ").append(filename).append(" ").append(length)
            .append(" b ").append(" from ")
            .append(fileSystem.getUri() + ", ")
            .append(infos[0].getDatanodeUuid())
            .append(Arrays.toString(infos)).append("\n");

    byte[] buffer;
    long totTime = 0;
    int tries = 3;
    for (int i = 0; i < tries; i++) {
      buffer = new byte[length];
      LOG.info("----------------------------start run "+i+"------------------------------------");
      long startTime = System.currentTimeMillis();

      in.readFully(0, buffer, 0, length);

      long endTime = System.currentTimeMillis() - startTime;
      // remove outliers
      //if(endTime < limit) {
        totTime += endTime;
      //}
      LOG.info("----------------------------finish run "+i+"------------------------------------");
      sb.append(endTime).append("\n");
    }
    in.close();
    fileSystem.close();

    sb.append(filename).append(" avg ").append(totTime / tries).append("\n").append("\n");
    String str = sb.toString();
    LOG.info(str);

    return str;
  }

  @Test
  public void testTimeGetLocatedBlocks() throws Exception {
    String filename = "getLocatedBlocks.txt";
    File file = s3Util.createLocalFile(filename, 1024);
    s3Util.uploadFile(BUCKET_NAME, filename, file);

    test.createImage(new FSTreeWalk(new Path(BUCKET_PATH), conf), nnDirPath, FixedBlockResolver.class);

    MiniDFSCluster cluster = test.startCluster(nnDirPath, 2,
            null,
            new StorageType[][]{
                    {StorageType.PROVIDED, StorageType.DISK},
                    {StorageType.DISK}
            },
            false, null);

    DFSClient client = new DFSClient(new InetSocketAddress("localhost",
            cluster.getNameNodePort()), cluster.getConfiguration(0));

    timeGetLocatedBlocks(client, "/"+filename, 1);

  }

  // GABRIEL - get located blocks only goes to NDB so this test is stupid
  private String timeGetLocatedBlocks(DFSClient client, String filename, int tries) throws IOException {
    long totTime = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tries; i++) {
      LOG.info("----------------------------start run "+i+"------------------------------------");
      long startTime = System.currentTimeMillis();
      LocatedBlocks locatedBlocks = client.getLocatedBlocks(filename, 0, filename.length());
      long endTime = System.currentTimeMillis() - startTime;
      totTime += endTime;

      LOG.info("----------------------------finish run "+i+"------------------------------------");
      // sb.append(endTime +"\n");
      sb.append("read ").append(filename).append(" from ").append(locatedBlocks.get(0).getLocations()[0].getDatanodeUuid())
              .append(Arrays.toString(locatedBlocks.get(0).getLocations())).append("\n")
              .append("run ").append(i).append(" took ").append(endTime).append(" ms").append("\n");
      LOG.info(sb.toString());
    }
    sb.append(filename).append(" avg ms time of ").append(tries).append(" tries : ").append(totTime / tries).append("\n").append("\n");
    LOG.info(sb.toString());

    return sb.toString();
  }


  @Test
  public void benchmarkBytes() {

  }

  @Test
  public void benchmarkHierarchy() {

  }

  @Test
  public void uploadFileToS3() {
    int fileID = 2;
    int size = 1;
    String filename;
    filename = "test-file-" + fileID + ".txt";
    File file = s3Util.createLocalFile(filename, size);
    s3Util.uploadFile(BUCKET_NAME, filename, file);
  }

  // "time" methods are benchmarked
  //@Benchmark
  public void getFileFromS3(int reps) throws Exception {
    for (int i = 0; i < reps; i++) {
      test.createImage(new FSTreeWalk(new Path(BUCKET_PATH), conf), nnDirPath,
              FixedBlockResolver.class);
    }
  }

  public void scanBucket(int reps) throws Exception {
    //  for (int i = 0; i < reps; i++) {
    test.createImage(new FSTreeWalk(new Path(BUCKET_PATH), conf), nnDirPath,
            FixedBlockResolver.class);
    //  }
  }
}