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

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.adaptor.BlockInfoDALAdaptor;
import io.hops.metadata.adaptor.INodeDALAdaptor;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility crawling an existing hierarchical FileSystem and emitting
 * a valid FSImage/NN storage.
 */
// TODO: generalize to types beyond FileRegion
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ImageWriter implements Closeable {

//  private static final int ONDISK_VERSION = 1;
//  private static final int LAYOUT_VERSION = -64; // see NameNodeLayoutVersion

  private final int startBlock;
  private final int startInode;
  private final UGIResolver ugis;
  private final BlockAliasMap.Writer<FileRegion> aliasMapWriter;
  private final BlockResolver blockIds;

  private boolean closed = false;
  private int curSec;
  private int curBlock;
  private final AtomicInteger curInode;

  public static Options defaults() {
    return new Options();
  }

  @SuppressWarnings("unchecked")
  public ImageWriter(Options opts) throws IOException {
    // GABRIEL - Do we need to create NNStorage and set NameSpaceInfo?
    String blockPoolID = opts.blockPoolID;

    startBlock = opts.startBlock;
    curBlock = startBlock;
    startInode = opts.startInode;
    curInode = new AtomicInteger(startInode);

    ugis = null == opts.ugis
      ? ReflectionUtils.newInstance(opts.ugisClass, opts.getConf())
      : opts.ugis;
    BlockAliasMap<FileRegion> fmt = null == opts.blocks
        ? ReflectionUtils.newInstance(opts.aliasMap, opts.getConf())
      : opts.blocks;
    aliasMapWriter = fmt.getWriter(null, blockPoolID);
    blockIds = null == opts.blockIds
      ? ReflectionUtils.newInstance(opts.blockIdsClass, opts.getConf())
      : opts.blockIds;
  }

  public INode accept(TreePath e) throws IOException {
    assert e.getParentId() < curInode.get();
    // allocate ID
    if(e.getParentId()>0){
      int id = curInode.getAndIncrement();
      e.accept(id);
      assert e.getId() < curInode.get();
      INode n = e.toINode(ugis, blockIds, aliasMapWriter);

      n.setParentIdNoPersistance(e.getParentId()); // GABRIEL - is parent id enough or do we need to set parent inode?
      return n;
    } else {
      e.accept(1);
      return null;
    }
  }

  void persistInodes(List<INode> inodes) throws IOException {
    Log.info("About to persist " + inodes.size() + " inodes...");
    new LightWeightRequestHandler(HDFSOperationType.ADD_INODE) {
      @Override
      public Object performTask() throws IOException {
        INodeDataAccess da = (INodeDataAccess) HdfsStorageFactory
                .getDataAccess(INodeDataAccess.class);

        da.prepare(null, inodes, null);

        return null;
      }
    }.handle();
    Log.info("Persisted inodes");
  }

  void persistBlocks(List<BlockInfo> blocks) throws IOException {
    Log.info("About to persist " + blocks.size() + " blocks...");
    new LightWeightRequestHandler(HDFSOperationType.ADD_SAFE_BLOCKS) {
      @Override
      public Object performTask() throws IOException {
        BlockInfoDataAccess da = (BlockInfoDataAccess) HdfsStorageFactory
                .getDataAccess(BlockInfoDataAccess.class);

        da.prepare(null, blocks, null); // throws java.lang.ClassCastException: io.hops.metadata.hdfs.entity.BlockInfo cannot be cast to org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo

        return null;
      }
    }.handle();
    Log.info("Persisted blocks");
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\", startBlock=").append(startBlock);
    sb.append(", curBlock=").append(curBlock);
    sb.append(", startInode=").append(startInode);
    sb.append(", curInode=").append(curInode);
    sb.append(", ugi=").append(ugis);
    sb.append(", blockIds=").append(blockIds);
    sb.append(" }");
    return sb.toString();
  }

  /**
   * Configurable options for image generation mapping pluggable components.
   */
  public static class Options implements Configurable {

    public static final String START_INODE = "hdfs.image.writer.start.inode";
    public static final String CACHE_ENTRY = "hdfs.image.writer.cache.entries";
    public static final String UGI_CLASS   = "hdfs.image.writer.ugi.class";
    public static final String BLOCK_RESOLVER_CLASS =
        "hdfs.image.writer.blockresolver.class";

    private Path outdir;
    private Configuration conf;
    private OutputStream outStream;
    private int maxdircache;
    private int startBlock;
    private int startInode;
    private UGIResolver ugis;
    private Class<? extends UGIResolver> ugisClass;
    private BlockAliasMap<FileRegion> blocks;
    private String clusterID;
    private String blockPoolID;

    @SuppressWarnings("rawtypes")
    private Class<? extends BlockAliasMap> aliasMap;
    private BlockResolver blockIds;
    private Class<? extends BlockResolver> blockIdsClass;
    private FSImageCompression compress =
        FSImageCompression.createNoopCompression();

    protected Options() {
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String def = new File("hdfs/name").toURI().toString();
      outdir = new Path(conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, def));
      startBlock = conf.getInt(FixedBlockResolver.START_BLOCK, (1 << 30) + 1);
      startInode = conf.getInt(START_INODE, (1 << 14) + 1);
      maxdircache = conf.getInt(CACHE_ENTRY, 100);
      ugisClass = conf.getClass(UGI_CLASS,
          SingleUGIResolver.class, UGIResolver.class);
      aliasMap = conf.getClass(
          DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
          NullBlockAliasMap.class, BlockAliasMap.class);
      blockIdsClass = conf.getClass(BLOCK_RESOLVER_CLASS,
          FixedBlockResolver.class, BlockResolver.class);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    public Options output(String out) {
      this.outdir = new Path(out);
      return this;
    }

    public Options outStream(OutputStream outStream) {
      this.outStream = outStream;
      return this;
    }

    public Options codec(String codec) throws IOException {
      this.compress = FSImageCompression.createCompression(getConf(), codec);
      return this;
    }

    public Options cache(int nDirEntries) {
      this.maxdircache = nDirEntries;
      return this;
    }

    public Options ugi(UGIResolver ugis) {
      this.ugis = ugis;
      return this;
    }

    public Options ugi(Class<? extends UGIResolver> ugisClass) {
      this.ugisClass = ugisClass;
      return this;
    }

    public Options blockIds(BlockResolver blockIds) {
      this.blockIds = blockIds;
      return this;
    }

    public Options blockIds(Class<? extends BlockResolver> blockIdsClass) {
      this.blockIdsClass = blockIdsClass;
      return this;
    }

    public Options blocks(BlockAliasMap<FileRegion> blocks) {
      this.blocks = blocks;
      return this;
    }

    @SuppressWarnings("rawtypes")
    public Options blocks(Class<? extends BlockAliasMap> blocksClass) {
      this.aliasMap = blocksClass;
      return this;
    }

    public Options clusterID(String clusterID) {
      this.clusterID = clusterID;
      return this;
    }

    public Options blockPoolID(String blockPoolID) {
      this.blockPoolID = blockPoolID;
      return this;
    }

  }
}
