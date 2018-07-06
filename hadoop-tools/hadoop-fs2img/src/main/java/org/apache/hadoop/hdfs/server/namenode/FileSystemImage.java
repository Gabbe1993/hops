/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Create FSImage from an external namespace.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class FileSystemImage implements Tool {

  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // require absolute URI to write anywhere but local
    FileSystem.setDefaultUri(conf, new File(".").toURI().toString());
  }

  protected void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("fs2img [OPTIONS] URI", new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("Options", options());
      ToolRunner.printGenericCommandUsage(System.out);
  }

  static Options options() {
    Options options = new Options();
    options.addOption("o", "outdir", true, "Output directory");
    options.addOption("u", "ugiclass", true, "UGI resolver class");
    options.addOption("b", "blockclass", true, "Block output class");
    options.addOption("i", "blockidclass", true, "Block resolver class");
    options.addOption("c", "cachedirs", true, "Max active dirents");
    options.addOption("cid", "clusterID", true, "Cluster ID");
    options.addOption("bpid", "blockPoolID", true, "Block Pool ID");
    options.addOption("h", "help", false, "Print usage");
    return options;
  }

  @Override
  public int run(String[] argv) throws Exception {
    Options options = options();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println(
        "Error parsing command-line options: " + e.getMessage());
      printUsage();
      return -1;
    }

    if (cmd.hasOption("h")) {
      printUsage();
      return -1;
    }

    ImageWriter.Options opts =
      ReflectionUtils.newInstance(ImageWriter.Options.class, getConf());
    for (Option o : cmd.getOptions()) {
      switch (o.getOpt()) {
        case "o":
          opts.output(o.getValue());
          break;
        case "u":
          opts.ugi(Class.forName(o.getValue()).asSubclass(UGIResolver.class));
          break;
        case "b":
          opts.blocks(
            Class.forName(o.getValue()).asSubclass(BlockAliasMap.class));
          break;
        case "i":
          opts.blockIds(
              Class.forName(o.getValue()).asSubclass(BlockResolver.class));
          break;
        case "c":
          opts.cache(Integer.parseInt(o.getValue()));
          break;
      case "cid":
        opts.clusterID(o.getValue());
        break;
      case "bpid":
        opts.blockPoolID(o.getValue());
        break;
        default:
        throw new UnsupportedOperationException(
            "Unknown option: " + o.getOpt());
      }
    }

    String[] rem = cmd.getArgs();
    if (rem.length != 1) {
      printUsage();
      return -1;
    }

    List<INode> inodes = new ArrayList<>();
    List<BlockInfo> blocks = new ArrayList<>();

    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : new FSTreeWalk(new Path(rem[0]), getConf())) {
        INode inode = w.accept(e);
        if (inode != null) {
          inodes.add(inode);
          if (inode instanceof INodeFile) {
            blocks.addAll(e.getBlockInfos());  // GABRIEL - test. only adding blocks if file and not directory
          }
        }
      }
      w.close();
      w.persistBlocks(blocks);
      w.persistInodesAndUsers(inodes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }




  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new FileSystemImage(), argv);
    System.exit(ret);
  }

}
