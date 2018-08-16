package org.apache.hadoop.hdfs.server.namenode;

import com.amazonaws.services.s3.AmazonS3;
import com.google.caliper.runner.CaliperMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
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

//@VmOptions("-XX:-TieredCompilation")
public class ProvidedBenchmark {


  public static void main(String[] args) throws Exception {
    String[] as = {"--trials", "2"};
    CaliperMain.main(ProvidedBenchmark.class, as);
  }

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
  private final String BUCKET_NAME = "benchmark-provided-bucket"; //
  private final String BUCKET_PATH = "s3a://" + BUCKET_NAME + "/";
  private final String RESULTS_DIR = "/home/gabriel/Documents/hops/benchmarks";

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
    String fileName = "/home/gabriel/Documents/hops/hadoop-tools/hadoop-fs2img/benchmark--976230672.txt";
    List<String> list = new ArrayList<>();
    try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
      list = stream
              .map(line -> line.split("=")[1]
                      .replace(" ms", "")
              )
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
  public void benchmarkReplication() throws Exception {
    int replication = 4;

    String remoteFilename = "remoteFile.txt";
    int baseFileLen = 1024;
    String baseDir = "/";

    File file = s3Util.createLocalFile(remoteFilename, baseFileLen);
    s3Util.uploadFile(BUCKET_NAME, remoteFilename, file);
    s3.putObject(BUCKET_NAME, remoteFilename, file);
    file.delete();

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

    String localFilename = "/local.txt";
    test.createFile(new Path(localFilename), (short) 1, baseFileLen, baseFileLen);

    String resultFile = "benchmarkReplication-" + new Date().getTime() + ".txt";
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(RESULTS_DIR, resultFile), true));
    int tries = 10;
    writer.write(timeGetLocatedBlocks(client, localFilename, tries));
    writer.write(timeGetLocatedBlocks(client, "/"+remoteFilename, tries));
    writer.close();


//    List<LocatedBlock> locatedBlocks = client.getLocatedBlocks(baseDir + filename, 0, baseFileLen).getLocatedBlocks();
//    ArrayList<DataNode> dns = cluster.getDataNodes();

  /*  int tries = 1;
    for (int i = 0; i < locatedBlocks.size(); i++) {
      LocatedBlock block = locatedBlocks.get(i);
      StorageType[] storageTypes = block.getStorageTypes();
      DatanodeInfo[] infos = block.getLocations();

      for (int j = 0; j < infos.length; j++) {
        DatanodeInfoWithStorage dn = (DatanodeInfoWithStorage) infos[j];
        if(dn.getStorageType().equals(StorageType.PROVIDED)) {
          DFSInputStream inputStream = client.open(filename);
          //inputStream.read
        }
      }
      if (storageTypes[i].equals(StorageType.PROVIDED)) {

      }
      Assert.assertNotNull(locatedBlocks);
    }*/
   // String read = test.readFromFile(cluster.getFileSystem(), "/" + filename);
   // Assert.assertEquals(toWrite, read);
  }

  private String timeGetLocatedBlocks(DFSClient client, String filename, int tries) throws IOException {
    long totTime = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tries; i++) {
      long startTime = System.currentTimeMillis();
      LocatedBlocks locatedBlocks = client.getLocatedBlocks(filename, 0, filename.length());
      long endTime = System.currentTimeMillis() - startTime;
      totTime += endTime;

      sb.append("read ").append(filename).append(" from ").append(locatedBlocks.get(0).getLocations()[0].getDatanodeUuid())
              .append(Arrays.toString(locatedBlocks.get(0).getLocations())).append("\n");
      sb.append("run ").append(i).append(" took ").append(endTime).append(" ms").append("\n");
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