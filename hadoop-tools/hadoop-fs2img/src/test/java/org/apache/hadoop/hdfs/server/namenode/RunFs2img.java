package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

import static java.lang.System.exit;

public class RunFs2img {

  public static void main(String[] args) {

    System.setProperty("hadoop.home.dir", "/");

    String path = "/home/gabriel/Documents/hops/hadoop-run/";
    String env  = path + "env.sh";
    String start = path + "start.sh";

    Runtime rt = Runtime.getRuntime();
    try {
      // open execute permission
      rt.exec("chmod +x " + env);
      rt.exec(env);
      rt.exec("chmod +x  " + start);
      rt.exec(start);

      // copy config
      rt.exec("java -cp \"$CP\" org.apache.hadoop.hdfs.server.namenode.FileSystemImage -conf \"$tmpconf\" \"$@\"");
    } catch (IOException e) {
      e.printStackTrace();
      exit(-1);
    }

    if(args.length == 0) {
      // default parameters
      args = new String[]{"-b", "org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap", "s3a://provided-test-ireland/"};
    }
    System.out.println("args: " + Arrays.toString(args));
    int ret = 0;
    try {
      // run tool
      ret = ToolRunner.run(new FileSystemImage(), args); // need to run via ToolRunner to populate ImageWriter.Options

      // move files to run directory
      rt.exec("mv /home/gabriel/Documents/hops/hdfs/ /home/gabriel/Documents/hops/hadoop-run/");
      rt.exec("mv /home/gabriel/Documents/hops/blocks.csv /home/gabriel/Documents/hops/hadoop-run/");

      /*
      // now start a namenode and load the fsimage
      rt.exec(env);
      rt.exec(export_nn);
      rt.exec(start_nn);
      */

     /* Configuration conf = new HdfsConfiguration();
      //conf.set(DFS_NAMENODE_NAME_DIR_KEY, nspath.toString());
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
              .format(false)
              .manageNameDfsDirs(false)
              .numDataNodes(1)
              .build();
      cluster.waitActive();*/

    } catch (Exception e) {
      e.printStackTrace();
    }

    exit(ret);
  }
}
