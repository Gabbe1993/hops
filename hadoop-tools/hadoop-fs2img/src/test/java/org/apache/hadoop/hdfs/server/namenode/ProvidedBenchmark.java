package org.apache.hadoop.hdfs.server.namenode;

import com.google.caliper.Runner;
import io.hops.metadata.HdfsStorageFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.google.caliper.SimpleBenchmark;
import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;

/**
 * Created by gabriel on 2018-08-08.
 */
public class ProvidedBenchmark extends SimpleBenchmark {

  public static void main(String[] args) throws Exception {
    new Runner().run(
            // These are the command line arguments for Runner. You can add
            // "--trials", "10" to run each benchmark 10 times each.
            "--trials", "10",
            ProvidedBenchmark.class.getName()
    );
  }

  @Rule
  public TestName name = new TestName();
  public static final Logger LOG =
          LoggerFactory.getLogger(ITestProvidedImplementation.class);

  private final Random r = new Random();
  private final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
  private final Path pBASE = new Path(fBASE.toURI().toString());
  private final Path providedPath = new Path(pBASE, "providedDir"); // folder for created provided files
  private final Path nnDirPath = new Path(pBASE, "nnDir"); // folder for .csv file
  private final String singleUser = "usr1";
  private final String singleGroup = "grp1";
  private String bpid = "";
  private Configuration conf;
  private MiniDFSCluster cluster;
  String bucket = "s3a://provided-test-ireland/";

  public ProvidedBenchmark() {
    try {
      setSeed();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

   @Before
  public void setSeed() throws Exception {
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }
    ((Log4JLogger) NameNode.blockStateChangeLog).getLogger().setLevel(Level.ALL);
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
    conf = new HdfsConfiguration();
    conf.set(SingleUGIResolver.USER, singleUser);
    conf.set(SingleUGIResolver.GROUP, singleGroup);

    conf.set(DFSConfigKeys.DFS_PROVIDER_STORAGEUUID,
            DFSConfigKeys.DFS_PROVIDER_STORAGEUUID_DEFAULT);
    // GABRIEL - setting DFS_NAMENODE_PROVIDED_ENABLED to true will create a default PROVIDED storage and datanode
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);

    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
            TextFileRegionAliasMap.class, BlockAliasMap.class);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR,
            nnDirPath.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
            new Path(nnDirPath, fileNameFromBlockPoolID(bpid)).toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_DELIMITER, ",");

    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR_PROVIDED,
            new File(providedPath.toUri()).toString());

    HdfsStorageFactory.setConfiguration(conf);
    HdfsStorageFactory.formatStorage();
    DFSTestUtil.formatNameNode(conf);
  }

  // "time" methods are benchmarked
  public void timeGetFileFromS3(int reps) throws Exception {
    for (int i = 0; i < reps; i++) {
      createImage(new FSTreeWalk(new Path(bucket), conf), nnDirPath,
              FixedBlockResolver.class);
    }
  }

  ImageWriter createImage(TreeWalk t, Path out,
                          Class<? extends BlockResolver> blockIdsClass) throws Exception {
    return createImage(t, out, blockIdsClass, "", TextFileRegionAliasMap.class);
  }

  ImageWriter createImage(TreeWalk t, Path out,
                          Class<? extends BlockResolver> blockIdsClass, String clusterID,
                          Class<? extends BlockAliasMap> aliasMapClass) throws Exception {
    ImageWriter.Options opts = ImageWriter.defaults();
    opts.setConf(conf);
    opts.output(out.toString())
            .blocks(aliasMapClass)
            .blockIds(blockIdsClass)
            .clusterID(clusterID)
            .blockPoolID(bpid);

    ArrayList<INode> inodes = new ArrayList<>();
    ArrayList<BlockInfo> blocks = new ArrayList<>();

    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : t) {
        INode inode = w.accept(e);
        if (inode != null) {
          inodes.add(inode);
          if (inode instanceof INodeFile) {
            blocks.addAll(e.getBlockInfos());
          }
        }
      }
      LOG.info("found "+ inodes.size()+" inodes and " + blocks.size() + " blocks from fs2img");
      w.close();

      w.persistBlocks(blocks); // make sure to start cluster before persisting
      w.persistInodesAndUsers(inodes);
      return w;
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }
}

