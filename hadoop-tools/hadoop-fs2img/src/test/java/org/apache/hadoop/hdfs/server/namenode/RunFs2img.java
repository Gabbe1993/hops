package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

import static java.lang.System.exit;

public class RunFs2img {

  public int run(String[] args) {
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

    if(args == null || args.length == 0) {
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

    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret;
  }

  public static void main(String[] args) {
    new RunFs2img().run(args);
  }
}
