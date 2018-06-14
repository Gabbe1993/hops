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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

import io.hops.common.Pair;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.adaptor.BlockInfoDALAdaptor;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;

import static org.apache.hadoop.hdfs.DFSUtil.LOG;
import static org.apache.hadoop.hdfs.DFSUtil.string2Bytes;

/**
 * Traversal cursor in external filesystem.
 * TODO: generalize, move FS/FileRegion to FSTreePath
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class TreePath {
  private int id = -1;
  private final int parentId;
  private final FileStatus stat;
  private final TreeWalk.TreeIterator i;
  private final FileSystem fs;
  private List<BlockInfo> blocks = new ArrayList<>();

  public static final long DEFAULT_NAMESPACE_QUOTA = Long.MAX_VALUE; // from DirectoryWithQuotaFeature
  public static final long DEFAULT_STORAGE_SPACE_QUOTA = HdfsConstants.QUOTA_RESET; // from DirectoryWithQuotaFeature

  protected TreePath(FileStatus stat, int parentId, TreeWalk.TreeIterator i,
      FileSystem fs) {
    this.i = i;
    this.stat = stat;
    this.parentId = parentId;
    this.fs = fs;
  }

  public FileStatus getFileStatus() {
    return stat;
  }

  public int getParentId() {
    return parentId;
  }

  public long getId() {
    if (id < 0) {
      throw new IllegalStateException();
    }
    return id;
  }

  void accept(int id) {
    this.id = (id);
    i.onAccept(this, id);
  }

  public INode toINode(UGIResolver ugi, BlockResolver blk,
      BlockAliasMap.Writer<FileRegion> out) throws IOException {
    if (stat.isFile()) {
      return toFile(ugi, blk, out);
    } else if (stat.isDirectory()) {
      return toDirectory(ugi);
    } else if (stat.isSymlink()) {
      throw new UnsupportedOperationException("symlinks not supported");
    } else {
      throw new UnsupportedOperationException("Unknown type: " + stat);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TreePath)) {
      return false;
    }
    TreePath o = (TreePath) other;
    return getParentId() == o.getParentId()
      && getFileStatus().equals(o.getFileStatus());
  }

  @Override
  public int hashCode() {
    long pId = getParentId() * getFileStatus().hashCode();
    return (int)(pId ^ (pId >>> 32));
  }

  void writeBlock(long blockId, long offset, long length, long genStamp,
      PathHandle pathHandle, BlockAliasMap.Writer<FileRegion> out)
      throws IOException {
    FileStatus s = getFileStatus();
    out.store(new FileRegion(blockId, s.getPath(), offset, length, genStamp,
        (pathHandle != null ? pathHandle.toByteArray() : new byte[0])));
  }

  INode toFile(UGIResolver ugi, BlockResolver blk,
      BlockAliasMap.Writer<FileRegion> out) throws IOException {
    final FileStatus s = getFileStatus();
    ugi.addUser(s.getOwner());
    ugi.addGroup(s.getGroup());
    INodeFile inode = new INodeFile(
            new PermissionStatus(ugi.user(s), ugi.group(s), ugi.permission(s)),
            null,
            s.getReplication(),
            s.getModificationTime(),
            s.getAccessTime(),
            blk.preferredBlockSize(s),
            HdfsConstants.PROVIDED_STORAGE_POLICY_ID);

    // pathhandle allows match as long as the file matches exactly.
    PathHandle pathHandle = null;
    if (fs != null) {
      try {
        pathHandle = fs.getPathHandle(s, Options.HandleOpt.exact());
      } catch (UnsupportedOperationException e) {
        LOG.warn(
            "Exact path handle not supported by filesystem " + fs.toString());
      }
    }
    // TODO: storage policy should be configurable per path; use BlockResolver
    long off = 0L;
    Iterable<BlockInfo> blks = blk.resolve(s);
    for (BlockInfo block : blk.resolve(s)) {
      blocks.add(block);
      writeBlock(block.getBlockId(), off, block.getNumBytes(),
          block.getGenerationStamp(), pathHandle, out);
      off += block.getNumBytes();
    }
    inode.setIdNoPersistance(id);
    inode.setLocalNameNoPersistance(string2Bytes(s.getPath().getName()));
    return inode;
  }

  INode toDirectory(UGIResolver ugi) throws IOException {
    final FileStatus s = getFileStatus();
    ugi.addUser(s.getOwner());
    ugi.addGroup(s.getGroup());

    INodeDirectory dir = new INodeDirectory(
            string2Bytes(s.getPath().getName()),
            new PermissionStatus(ugi.user(s), ugi.group(s), ugi.permission(s)),
            s.getModificationTime());

    INodeDirectoryWithQuota dirQ = new INodeDirectoryWithQuota(
            DEFAULT_NAMESPACE_QUOTA,
            DEFAULT_STORAGE_SPACE_QUOTA,
            dir);
    dirQ.setIdNoPersistance(id);
    dirQ.setLocalNameNoPersistance(string2Bytes(s.getPath().getName()));

    return dirQ;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ stat=\"").append(getFileStatus()).append("\"");
    sb.append(", id=").append(getId());
    sb.append(", parentId=").append(getParentId());
    sb.append(", iterObjId=").append(System.identityHashCode(i));
    sb.append(" }");
    return sb.toString();
  }

  public List<BlockInfo> getBlockInfos() {
    return blocks;
  }
}
