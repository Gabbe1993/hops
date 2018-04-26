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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DU;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A block pool slice represents a portion of a block pool stored on a volume.
 * Taken together, all BlockPoolSlices sharing a block pool ID across a
 * cluster represent a single block pool.
 * <p/>
 * This class is synchronized by {@link FsVolumeImpl}.
 */
class BlockPoolSlice {
  private final String bpid;
  private final FsVolumeImpl volume;
      // volume to which this BlockPool belongs to
  private final File currentDir; // StorageDirectory/current/bpid/current
  private final File finalizedDir; // directory store Finalized replica
  private final File rbwDir; // directory store RBW replica
  private final File tmpDir; // directory store Temporary replica
  private final FileIoProvider fileIoProvider;
  private AtomicLong numOfBlocks = new AtomicLong();

  // TODO:FEDERATION scalability issue - a thread per DU is needed
  private final DU dfsUsage;

  /**
   * Create a blook pool slice
   *
   * @param bpid
   *     Block pool Id
   * @param volume
   *     {@link FsVolumeImpl} to which this BlockPool belongs to
   * @param bpDir
   *     directory corresponding to the BlockPool
   * @param conf
   * @throws IOException
   */
  BlockPoolSlice(String bpid, FsVolumeImpl volume, File bpDir,
      Configuration conf) throws IOException {
    this.bpid = bpid;
    this.volume = volume;
    this.currentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    this.finalizedDir = new File(
            currentDir, DataStorage.STORAGE_DIR_FINALIZED);
    if (!this.finalizedDir.exists()) {
      if (!this.finalizedDir.mkdirs()) {
        throw new IOException("Failed to mkdirs " + this.finalizedDir);
      }
    }
    this.fileIoProvider = volume.getFileIoProvider();

    // Files that were being written when the datanode was last shutdown
    // are now moved back to the data directory. It is possible that
    // in the future, we might want to do some sort of datanode-local
    // recovery for these blocks. For example, crc validation.
    //
    this.tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
    if (tmpDir.exists()) {
      fileIoProvider.fullyDelete(volume, tmpDir);
    }
    this.rbwDir = new File(currentDir, DataStorage.STORAGE_DIR_RBW);
    final boolean supportAppends =
        conf.getBoolean(DFSConfigKeys.DFS_SUPPORT_APPEND_KEY,
            DFSConfigKeys.DFS_SUPPORT_APPEND_DEFAULT);
    if (rbwDir.exists() && !supportAppends) {
      FileUtil.fullyDelete(rbwDir);
    }
    final int maxBlocksPerDir =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_KEY,
            DFSConfigKeys.DFS_DATANODE_NUMBLOCKS_DEFAULT);

    if (!rbwDir.mkdirs()) {  // create rbw directory if not exist
      if (!rbwDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + rbwDir.toString());
      }
    }
    if (!tmpDir.mkdirs()) {
      if (!tmpDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + tmpDir.toString());
      }
    }
    this.dfsUsage = new DU(bpDir, conf);
    this.dfsUsage.start();
  }

  File getDirectory() {
    return currentDir.getParentFile();
  }

  File getFinalizedDir() {
    return finalizedDir;
  }
  
  File getRbwDir() {
    return rbwDir;
  }

  /**
   * Run DU on local drives.  It must be synchronized from caller.
   */
  void decDfsUsed(long value) {
    dfsUsage.decDfsUsed(value);
  }
  
  long getDfsUsed() throws IOException {
    return dfsUsage.getUsed();
  }

  void incDfsUsed(long value) {
    // Not implemented since we do not have caching
    // if (dfsUsage instanceof CachingGetSpaceUsed) {
    //   ((CachingGetSpaceUsed)dfsUsage).incDfsUsed(value);
    // }
  }
  /**
   * Temporary files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createTmpFile(Block b) throws IOException {
    File f = new File(tmpDir, b.getBlockName());
    File tmpFile = DatanodeUtil.createFileWithExistsCheck(
        volume, b, f, fileIoProvider);
    // If any exception during creation, its expected that counter will not be
    // incremented, So no need to decrement
    incrNumBlocks();
    return tmpFile;
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(Block b) throws IOException {
    File f = new File(rbwDir, b.getBlockName());
    File rbwFile = DatanodeUtil.createFileWithExistsCheck(
            volume, b, f, fileIoProvider);
    // If any exception during creation, its expected that counter will not be
    // incremented, So no need to decrement
    incrNumBlocks();
    return rbwFile;
  }

  /**
   * Replaced addBlocks
   */
  File addFinalizedBlock(Block b, ReplicaInfo replicaInfo) throws IOException {
    File blockDir = DatanodeUtil.idToBlockDir(finalizedDir, b.getBlockId());
    fileIoProvider.mkdirsWithExistsCheck(volume, blockDir);
    File blockFile = FsDatasetImpl.moveBlockFiles(b, replicaInfo, blockDir);
    File metaFile = FsDatasetUtil.getMetaFile(blockFile, b.getGenerationStamp());

    return blockFile;
  }

  void checkDirs() throws DiskErrorException {
    DiskChecker.checkDir(finalizedDir);
    DiskChecker.checkDir(tmpDir);
    DiskChecker.checkDir(rbwDir);
  }

  void getVolumeMap(ReplicaMap volumeMap) throws IOException {
    // add finalized replicas
    addToReplicasMap(volumeMap, finalizedDir, true);
    // add rbw replicas
    addToReplicasMap(volumeMap, rbwDir, false);
  }

  /**
   * Add replicas under the given directory to the volume map
   *
   * @param volumeMap
   *     the replicas map
   * @param dir
   *     an input directory
   * @param isFinalized
   *     true if the directory has finalized replicas;
   *     false if the directory has rbw replicas
   */
  void addToReplicasMap(ReplicaMap volumeMap, File dir, boolean isFinalized)
      throws IOException {
    File blockFiles[] = FileUtil.listFiles(dir);
    for (File blockFile : blockFiles) {
      if (!Block.isBlockFilename(blockFile)) {
        continue;
      }

      long genStamp =
          FsDatasetUtil.getGenerationStampFromFile(blockFiles, blockFile);
      long blockId = Block.filename2id(blockFile.getName());
      Block block = new Block(blockId, blockFile.length(), genStamp);
      ReplicaInfo newReplica = null;
      if (isFinalized) {
        newReplica = new ReplicaBuilder(HdfsServerConstants.ReplicaState.FINALIZED)
                .setBlockId(blockId)
                .setLength(block.getNumBytes())
                .setGenerationStamp(genStamp)
                .setFsVolume(volume)
                .setDirectoryToUse(blockFile.getParentFile()) // TODO: GABRIEL - test if correct dir
                .build();
      } else {
        newReplica = new ReplicaBuilder(HdfsServerConstants.ReplicaState.RWR)
                .setBlockId(blockId)
                .setLength(validateIntegrity(blockFile, genStamp))
                .setGenerationStamp(genStamp)
                .setFsVolume(volume)
                .setDirectoryToUse(blockFile.getParentFile())
                .build();
      }

      ReplicaInfo oldReplica = volumeMap.add(bpid, newReplica);
      if (oldReplica != null) {
        FsDatasetImpl.LOG.warn("Two block files with the same block id exist " +
            "on disk: " + oldReplica.getBlockURI() + " and " + blockFile);
      }
    }
  }
  
  /**
   * Find out the number of bytes in the block that match its crc.
   * <p/>
   * This algorithm assumes that data corruption caused by unexpected
   * datanode shutdown occurs only in the last crc chunk. So it checks
   * only the last chunk.
   *
   * @param blockFile
   *     the block file
   * @param genStamp
   *     generation stamp of the block
   * @return the number of valid bytes
   */
  private long validateIntegrity(File blockFile, long genStamp) {
    DataInputStream checksumIn = null;
    InputStream blockIn = null;
    try {
      final File metaFile = FsDatasetUtil.getMetaFile(blockFile, genStamp);
      long blockFileLen = blockFile.length();
      long metaFileLen = metaFile.length();
      int crcHeaderLen = DataChecksum.getChecksumHeaderSize();
      if (!blockFile.exists() || blockFileLen == 0 ||
          !metaFile.exists() || metaFileLen < crcHeaderLen) {
        return 0;
      }
      checksumIn = new DataInputStream(
          new BufferedInputStream(new FileInputStream(metaFile),
              HdfsConstants.IO_FILE_BUFFER_SIZE));

      // read and handle the common header here. For now just a version
      BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
      short version = header.getVersion();
      if (version != BlockMetadataHeader.VERSION) {
        FsDatasetImpl.LOG.warn(
            "Wrong version (" + version + ") for metadata file " + metaFile +
                " ignoring ...");
      }
      DataChecksum checksum = header.getChecksum();
      int bytesPerChecksum = checksum.getBytesPerChecksum();
      int checksumSize = checksum.getChecksumSize();
      long numChunks =
          Math.min((blockFileLen + bytesPerChecksum - 1) / bytesPerChecksum,
              (metaFileLen - crcHeaderLen) / checksumSize);
      if (numChunks == 0) {
        return 0;
      }
      IOUtils.skipFully(checksumIn, (numChunks - 1) * checksumSize);
      blockIn = new FileInputStream(blockFile);
      long lastChunkStartPos = (numChunks - 1) * bytesPerChecksum;
      IOUtils.skipFully(blockIn, lastChunkStartPos);
      int lastChunkSize =
          (int) Math.min(bytesPerChecksum, blockFileLen - lastChunkStartPos);
      byte[] buf = new byte[lastChunkSize + checksumSize];
      checksumIn.readFully(buf, lastChunkSize, checksumSize);
      IOUtils.readFully(blockIn, buf, 0, lastChunkSize);

      checksum.update(buf, 0, lastChunkSize);
      if (checksum.compare(buf, lastChunkSize)) { // last chunk matches crc
        return lastChunkStartPos + lastChunkSize;
      } else { // last chunck is corrupt
        return lastChunkStartPos;
      }
    } catch (IOException e) {
      FsDatasetImpl.LOG.warn(e);
      return 0;
    } finally {
      IOUtils.closeStream(checksumIn);
      IOUtils.closeStream(blockIn);
    }
  }
  /**
   * This method is invoked during DN startup when volumes are scanned to
   * build up the volumeMap.
   *
   * Given two replicas, decide which one to keep. The preference is as
   * follows:
   *   1. Prefer the replica with the higher generation stamp.
   *   2. If generation stamps are equal, prefer the replica with the
   *      larger on-disk length.
   *   3. If on-disk length is the same, prefer the replica on persistent
   *      storage volume.
   *   4. All other factors being equal, keep replica1.
   *
   * The other replica is removed from the volumeMap and is deleted from
   * its storage volume.
   *
   * @param replica1
   * @param replica2
   * @param volumeMap
   * @return the replica that is retained.
   * @throws IOException
   */
  ReplicaInfo resolveDuplicateReplicas(
          final ReplicaInfo replica1, final ReplicaInfo replica2,
          final ReplicaMap volumeMap) throws IOException {

    // TODO: GABRIEL - implement. Now returning first replica

    return replica1;
  }

  @Override
  public String toString() {
    return currentDir.getAbsolutePath();
  }
  void shutdown() {
    dfsUsage.shutdown();
  }

  void incrNumBlocks() {
    numOfBlocks.incrementAndGet();
  }

  void decrNumBlocks() {
    numOfBlocks.decrementAndGet();
  }

   public long getNumOfBlocks() {
    return numOfBlocks.get();
  }
}