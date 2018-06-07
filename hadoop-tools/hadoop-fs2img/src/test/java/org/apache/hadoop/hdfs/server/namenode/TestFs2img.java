package test.java.org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

import static java.lang.System.exit;

/**
 * Created by gabriel on 2018-06-05.
 */
public class TestFs2img {

  public static void main(String[] args) {

    String path = "/home/gabriel/Documents/hadoop-prototype/hadoop-tools/hadoop-fs2img/hadoop-run/";
    String s1  = path + "env.sh";
    String s2 =  path + "export_conf.sh";
    try {
      Runtime.getRuntime().exec("chmod +x " + s1);
      Runtime.getRuntime().exec(s1);
      Runtime.getRuntime().exec("chmod +x  " + s2);
      Runtime.getRuntime().exec(s2);
//      Runtime.getRuntime().exec("CP=$(hadoop classpath):\"$HADOOP_HOME\"/share/hadoop/tools/lib/*");
      Runtime.getRuntime().exec("java -cp \"$CP\" org.apache.hadoop.hdfs.server.namenode.FileSystemImage -conf \"$tmpconf\" \"$@\"");
  //    exit(0);
    } catch (IOException e) {
      e.printStackTrace();
      exit(-1);
    }

    if(args.length == 0) {
      args = new String[]{"-b", "org.apache.hadoop.hdfs.server.common.TextFileRegionFormat", "s3a://provided-test/"};
    }
    System.out.println("args: " + Arrays.toString(args));
    int ret = 0;
    try {
      ret = ToolRunner.run(new FileSystemImage(), args); // need to run via ToolRunner to populate ImageWriter.Options
    } catch (Exception e) {
      e.printStackTrace();
    }
    exit(ret);
  }
}
