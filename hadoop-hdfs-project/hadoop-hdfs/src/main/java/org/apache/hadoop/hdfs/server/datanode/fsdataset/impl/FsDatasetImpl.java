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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.*;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ***********************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 * <p/>
 * *************************************************
 */
@InterfaceAudience.Private
class FsDatasetImpl implements FsDatasetSpi<FsVolumeImpl> {
  static final Log LOG = LogFactory.getLog(FsDatasetImpl.class);
  private static long XCEIVER_STOP_TIMEOUT;
  private final int NUM_BUCKETS;

  @Override // FsDatasetSpi
  public List<FsVolumeImpl> getVolumes() {
    return volumes.getVolumes();
  }
  @Override // FsDatasetSpi
  public StorageReport[] getStorageReports(String bpid)
      throws IOException {
    StorageReport[] reports;
    synchronized (statsLock) {
      List<FsVolumeImpl> curVolumes = getVolumes();
      reports = new StorageReport[curVolumes.size()];
      int i = 0;
      for (FsVolumeImpl volume : curVolumes) {
        reports[i++] = new StorageReport(volume.toDatanodeStorage(),
            false,
            volume.getCapacity(),
            volume.getDfsUsed(),
            volume.getAvailable(),
            volume.getBlockPoolUsed(bpid));
      }
    }

    return reports;
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    return storageMap.get(storageUuid);
  }

  @Override
  public synchronized FsVolumeImpl getVolume(final ExtendedBlock b) {
    final ReplicaInfo r = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    return r != null ? (FsVolumeImpl) r.getVolume() : null;
  }


  @Override // FsDatasetSpi
  public synchronized Block getStoredBlock(String bpid, long blkid)
      throws IOException {
    ReplicaInfo r = volumeMap.get(bpid, blkid);
      if (r == null) {
      return null;
    }
    return new Block(blkid, r.getBytesOnDisk(), r.getGenerationStamp());
  }

  /**
   * Returns a clone of a replica stored in data-node memory.
   * Should be primarily used for testing.
   *
   * @param blockId
   * @return
   */
  ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
    ReplicaInfo r = volumeMap.get(bpid, blockId);
    if (r == null) {
      return null;
    }
    switch (r.getState()) {
      case FINALIZED:
      case RBW:
      case RWR:
      case RUR:
      case TEMPORARY:
      return new ReplicaBuilder(r.getState()).from(r).build();
    }
    return null;
  }

  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    ReplicaInfo info = getBlockReplica(b);
    if (info == null || !info.metadataExists()) {
      return null;
    }
    return info.getMetadataInputStream(0);
  }

  final DataNode datanode;
  final DataStorage dataStorage;
  final FsVolumeList volumes;
  final Map<String, DatanodeStorage> storageMap;
  final ReplicaMap volumeMap;
  final FsDatasetAsyncDiskService asyncDiskService;
  private final Configuration conf;
  private final int validVolsRequired;

  // Used for synchronizing access to usage stats
  private final Object statsLock = new Object();

  /**
   * An FSDataset has a directory where it loads its data files.
   */
  FsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf)
      throws IOException {
    this.datanode = datanode;
    this.dataStorage = storage;
    this.conf = conf;

    // The number of volumes required for operation is the total number
    // of volumes minus the number of failed volumes we can tolerate.
    final int volFailuresTolerated =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
            DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);

    String[] dataDirs = conf.getTrimmedStrings(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    Collection<StorageLocation> dataLocations = DataNode.getStorageLocations(conf);

    int volsConfigured = (dataDirs == null) ? 0 : dataDirs.length;
    int volsFailed = volsConfigured - storage.getNumStorageDirs();
    this.validVolsRequired = volsConfigured - volFailuresTolerated;

    if (volFailuresTolerated < 0 || volFailuresTolerated >= volsConfigured) {
      throw new DiskErrorException(
          "Invalid volume failure " + " config value: " + volFailuresTolerated);
    }
    if (volsFailed > volFailuresTolerated) {
      throw new DiskErrorException(
          "Too many failed volumes - "
              + "current valid volumes: " + storage.getNumStorageDirs()
              + ", volumes configured: " + volsConfigured
              + ", volumes failed: " + volsFailed
              + ", volume failures tolerated: " + volFailuresTolerated);
    }


    storageMap = new ConcurrentHashMap<String, DatanodeStorage>();
    volumeMap = new ReplicaMap(this);

    @SuppressWarnings("unchecked")
    final VolumeChoosingPolicy<FsVolumeImpl> blockChooserImpl = ReflectionUtils
        .newInstance(conf.getClass(
            DFSConfigKeys.DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY,
            RoundRobinVolumeChoosingPolicy.class, VolumeChoosingPolicy.class),
            conf);

    volumes = new FsVolumeList(volsFailed, blockChooserImpl);
    asyncDiskService = new FsDatasetAsyncDiskService(datanode, this);

    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {
      addVolume(dataLocations, storage.getStorageDir(idx));
    }

    registerMBean(datanode.getDatanodeUuid());
    NUM_BUCKETS = conf.getInt(DFSConfigKeys.DFS_NUM_BUCKETS_KEY,
        DFSConfigKeys.DFS_NUM_BUCKETS_DEFAULT);
    XCEIVER_STOP_TIMEOUT = datanode.getDnConf().getXceiverStopTimeout();
  }

  private void addVolume(Collection<StorageLocation> dataLocations,
      Storage.StorageDirectory sd) throws IOException {
    final StorageLocation storageLocation = sd.getStorageLocation();

    // If IOException raises from FsVolumeImpl() or getVolumeMap(), there is
    // nothing needed to be rolled back to make various data structures, e.g.,
    // storageMap and asyncDiskService, consistent.l
   FsVolumeImpl fsVolume = new FsVolumeImplBuilder()
            .setDataset(this)
            .setStorageID(sd.getStorageUuid())
            .setStorageDirectory(sd)
            .setFileIoProvider(datanode.getFileIoProvider())
            .setConf(this.conf)
            .build();
    ReplicaMap tempVolumeMap = new ReplicaMap(this);
    synchronized (this) {
      volumeMap.addAll(tempVolumeMap);
      storageMap.put(sd.getStorageUuid(),
          new DatanodeStorage(sd.getStorageUuid(),
              DatanodeStorage.State.NORMAL,
              storageLocation.getStorageType()));
      asyncDiskService.addVolume(fsVolume);
      volumes.addVolume(fsVolume);
    }
    LOG.info("Added volume - " + storageLocation + ", StorageType: " +
            storageLocation.getStorageType());
  }

  @VisibleForTesting
  public FsVolumeImpl createFsVolume(String storageUuid,
                                     Storage.StorageDirectory sd,
                                     final StorageLocation location) throws IOException {
    return new FsVolumeImplBuilder()
            .setDataset(this)
            .setStorageID(storageUuid)
            .setStorageDirectory(sd)
            .setFileIoProvider(datanode.getFileIoProvider())
            .setConf(conf)
            .build();
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getDfsUsed() throws IOException {
    synchronized (statsLock) {
      return volumes.getDfsUsed();
    }
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    synchronized (statsLock) {
      return volumes.getBlockPoolUsed(bpid);
    }
  }

  /**
   * Return true - if there are still valid volumes on the DataNode.
   */
  @Override // FsDatasetSpi
  public boolean hasEnoughResource() {
    return getVolumes().size() >= validVolsRequired;
  }

  /**
   * Return total capacity, used and unused
   */
  @Override // FSDatasetMBean
  public long getCapacity() {
    synchronized (statsLock) {
      return volumes.getCapacity();
    }
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  @Override // FSDatasetMBean
  public long getRemaining() throws IOException {
    synchronized (statsLock) {
      return volumes.getRemaining();
    }
  }

  /**
   * Return the number of failed volumes in the FSDataset.
   */
  @Override
  public int getNumFailedVolumes() {
    return volumes.numberOfFailedVolumes();
  }

  /**
   * Find the block's on-disk length
   */
  @Override // FsDatasetSpi
  public long getLength(ExtendedBlock b) throws IOException {
    return getBlockReplica(b).getBlockDataLength();
  }

  /**
   * Get File name for a given block.
   */
  private ReplicaInfo getBlockReplica(ExtendedBlock b) throws IOException {
    return getBlockReplica(b.getBlockPoolId(), b.getBlockId());
  }

  /**
   * Get File name for a given block.
   */
  ReplicaInfo getBlockReplica(String bpid, long blockId) throws IOException {
    ReplicaInfo r = validateBlockFile(bpid, blockId);
    if (r == null) {
      throw new FileNotFoundException("BlockId " + blockId + " is not valid.");
    }
    return r;
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b, long seekOffset)
      throws IOException {
    ReplicaInfo info;
    synchronized(this) {
      info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    }

    if(info != null && info.blockDataExists()) {
      return info.getDataInputStream(seekOffset);
    } else {
      throw new IOException("No data exists for block " + b);
    }
  }

  /**
   * Get the meta info of a block stored in volumeMap. To find a block,
   * block pool Id, block Id and generation stamp must match.
   *
   * @param b
   *     extended block
   * @return the meta replica information; null if block was not found
   * @throws ReplicaNotFoundException
   *     if no entry is in the map or
   *     there is a generation stamp mismatch
   */
  ReplicaInfo getReplicaInfo(ExtendedBlock b) throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    if (info == null) {
      if (volumeMap.get(b.getBlockPoolId(), b.getLocalBlock().getBlockId())
              == null) {
        throw new ReplicaNotFoundException(
                ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
      } else {
        throw new ReplicaNotFoundException(
                ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b);
      }
    }
    return info;
  }

  /**
   * Get the meta info of a block stored in volumeMap. Block is looked up
   * without matching the generation stamp.
   *
   * @param bpid
   *     block pool Id
   * @param blkid
   *     block Id
   * @return the meta replica information; null if block was not found
   * @throws ReplicaNotFoundException
   *     if no entry is in the map or
   *     there is a generation stamp mismatch
   */

  ReplicaInfo getReplicaInfo(String bpid, long blkid)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(bpid, blkid);
    if (info == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + bpid + ":" + blkid);
    }
    return info;
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  @Override // FsDatasetSpi
  public synchronized ReplicaInputStreams getTmpInputStreams(ExtendedBlock b,
      long blkOffset, long metaOffset) throws IOException {
    ReplicaInfo info = getReplicaInfo(b);
    FsVolumeImpl ref = (FsVolumeImpl) info.getVolume();
    try {
      InputStream blockInStream = info.getDataInputStream(blkOffset);
      try {
        InputStream metaInStream = info.getMetadataInputStream(metaOffset);
        return new ReplicaInputStreams(
                blockInStream, metaInStream, ref, datanode.getFileIoProvider());
      } catch (IOException e) {
        IOUtils.cleanup(null, blockInStream);
        throw e;
      }
    } catch (IOException e) {
       throw e;
    }
  }

  static File moveBlockFiles(Block b, ReplicaInfo replicaInfo, File destdir)
          throws IOException {
    final File dstfile = new File(destdir, b.getBlockName());
    final File dstmeta = FsDatasetUtil.getMetaFile(dstfile, b.getGenerationStamp());
    try {
      replicaInfo.renameMeta(dstmeta.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to move meta file for " + b
              + " from " + replicaInfo.getMetadataURI() + " to " + dstmeta, e);
    }
    try {
      replicaInfo.renameData(dstfile.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to move block file for " + b
              + " from " + replicaInfo.getBlockURI() + " to "
              + dstfile.getAbsolutePath(), e);
    }
  //  if (LOG.isDebugEnabled()) {
      LOG.info("addFinalizedBlock: Moved " + replicaInfo.getMetadataURI()
              + "\n to " + dstmeta + "\n and " + replicaInfo.getBlockURI()
              + "\n to " + dstfile);
 //   }
    return dstfile;
  }

  static private void truncateBlock(File blockFile, File metaFile, long oldlen,
      long newlen) throws IOException {
    LOG.info(
        "truncateBlock: blockFile=" + blockFile + ", metaFile=" + metaFile +
            ", oldlen=" + oldlen + ", newlen=" + newlen);

    if (newlen == oldlen) {
      return;
    }
    if (newlen > oldlen) {
      throw new IOException("Cannot truncate block to from oldlen (=" + oldlen +
          ") to newlen (=" + newlen + ")");
    }

    DataChecksum dcs = BlockMetadataHeader.readHeader(metaFile).getChecksum();
    int checksumsize = dcs.getChecksumSize();
    int bpc = dcs.getBytesPerChecksum();
    long n = (newlen - 1) / bpc + 1;
    long newmetalen = BlockMetadataHeader.getHeaderSize() + n * checksumsize;
    long lastchunkoffset = (n - 1) * bpc;
    int lastchunksize = (int) (newlen - lastchunkoffset);
    byte[] b = new byte[Math.max(lastchunksize, checksumsize)];

    RandomAccessFile blockRAF = new RandomAccessFile(blockFile, "rw");
    try {
      //truncate blockFile
      blockRAF.setLength(newlen);

      //read last chunk
      blockRAF.seek(lastchunkoffset);
      blockRAF.readFully(b, 0, lastchunksize);
    } finally {
      blockRAF.close();
    }

    //compute checksum
    dcs.update(b, 0, lastchunksize);
    dcs.writeValue(b, 0, false);

    //update metaFile
    RandomAccessFile metaRAF = new RandomAccessFile(metaFile, "rw");
    try {
      metaRAF.setLength(newmetalen);
      metaRAF.seek(newmetalen - checksumsize);
      metaRAF.write(b, 0, checksumsize);
    } finally {
      metaRAF.close();
    }
  }

  @Override  // FsDatasetSpi
  public synchronized ReplicaInPipeline append(ExtendedBlock b, long newGS,
                                               long expectedBlockLen) throws IOException {
    // If the block was successfully finalized because all packets
    // were successfully processed at the Datanode but the ack for
    // some of the packets were not received by the client. The client
    // re-opens the connection and retries sending those packets.
    // The other reason is that an "append" is occurring to this block.

    // check the validity of the parameter
    if (newGS < b.getGenerationStamp()) {
      throw new IOException("The new generation stamp " + newGS +
          " should be greater than the replica " + b + "'s generation stamp");
    }
    ReplicaInfo replicaInfo = getReplicaInfo(b);
    LOG.info("Appending to " + replicaInfo);
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
    }
    if (replicaInfo.getNumBytes() != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo +
          " with a length of " + replicaInfo.getNumBytes() +
          " expected length is " + expectedBlockLen);
    }

    return append(b.getBlockPoolId(), replicaInfo, newGS,
        b.getNumBytes());
  }

  /**
   * Append to a finalized replica
   * Change a finalized replica to be a RBW replica and
   * bump its generation stamp to be the newGS
   *
   * @param bpid
   *     block pool Id
   * @param replicaInfo
   *     a finalized replica
   * @param newGS
   *     new generation stamp
   * @param estimateBlockLen
   *     estimate generation stamp
   * @return a RBW replica
   * @throws IOException
   *     if moving the replica from finalized directory
   *     to rbw directory fails
   */
  private synchronized ReplicaInPipeline append(String bpid,
                                                ReplicaInfo replicaInfo, long newGS, long estimateBlockLen)
      throws IOException {
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new IOException("Only a Finalized replica can be appended to; "
              + "Replica with blk id " + replicaInfo.getBlockId() + " has state "
              + replicaInfo.getState());
    }
    // If there are any hardlinks to the block, break them.  This ensures
    // we are not appending to a file that is part of a previous/ directory.
    replicaInfo.breakHardLinksIfNeeded();

    FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
    ReplicaInPipeline rip = v.append(bpid, replicaInfo,
            newGS, estimateBlockLen);
    if (rip.getReplicaInfo().getState() != ReplicaState.RBW) {
      throw new IOException("Append on block " + replicaInfo.getBlockId() +
              " returned a replica of state " + rip.getReplicaInfo().getState()
              + "; expected RBW");
    }
    // Replace finalized replica by a RBW replica in replicas map
    volumeMap.add(bpid, rip.getReplicaInfo());
    return rip;
  }

  private ReplicaInfo recoverCheck(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    ReplicaInfo replicaInfo =
        getReplicaInfo(b.getBlockPoolId(), b.getBlockId());

    // check state
    if (replicaInfo.getState() != ReplicaState.FINALIZED &&
        replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA +
              replicaInfo);
    }

    // check generation stamp
    long replicaGenerationStamp = replicaInfo.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA +
              replicaGenerationStamp + ". Expected GS range is [" +
              b.getGenerationStamp() + ", " +
              newGS + "].");
    }

    // stop the previous writer before check a replica's length
    long replicaLen = replicaInfo.getNumBytes();
    if (replicaInfo.getState() == ReplicaState.RBW) {
      ReplicaBeingWritten rbw = (ReplicaBeingWritten) replicaInfo;
      // kill the previous writer
      rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      rbw.setWriter(Thread.currentThread());
      // check length: bytesRcvd, bytesOnDisk, and bytesAcked should be the same
      if (replicaLen != rbw.getBytesOnDisk() ||
          replicaLen != rbw.getBytesAcked()) {
        throw new ReplicaAlreadyExistsException("RBW replica " + replicaInfo +
            "bytesRcvd(" + rbw.getNumBytes() + "), bytesOnDisk(" +
            rbw.getBytesOnDisk() + "), and bytesAcked(" + rbw.getBytesAcked() +
            ") are not the same.");
      }
    }

    // check block length
    if (replicaLen != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo +
          " with a length of " + replicaLen +
          " expected length is " + expectedBlockLen);
    }

    return replicaInfo;
  }

  @Override  // FsDatasetSpi
  public synchronized ReplicaInPipeline recoverAppend(ExtendedBlock b,
                                                      long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed append to " + b);

    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);

    // change the replica's state/gs etc.
    if (replicaInfo.getState() == ReplicaState.FINALIZED) {
      return append(b.getBlockPoolId(), replicaInfo, newGS,
          b.getNumBytes());
    } else { //RBW
      replicaInfo.bumpReplicaGS(newGS);
      return (ReplicaInPipeline) replicaInfo;
    }
  }

  @Override // FsDatasetSpi
  public String recoverClose(ExtendedBlock b, long newGS, long expectedBlockLen)
      throws IOException {
    LOG.info("Recover failed close " + b);
    // check replica's state
    ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
    // bump the replica's GS
    replicaInfo.bumpReplicaGS(newGS);
    // finalize the replica if RBW
    if (replicaInfo.getState() == ReplicaState.RBW) {
      finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
    return replicaInfo.getStorageUuid();
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline createRbw(StorageType storageType,
                                                  ExtendedBlock b) throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
        b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " already exists in state " + replicaInfo.getState() +
          " and thus cannot be created.");
    }
    // create a new block
    FsVolumeImpl v = volumes.getNextVolume(storageType, b.getNumBytes());

    ReplicaInPipeline newReplicaInfo;
    try {
      newReplicaInfo = v.createRbw(b);
      if (newReplicaInfo.getReplicaInfo().getState() != ReplicaState.RBW) {
        throw new IOException("CreateRBW returned a replica of state "
                + newReplicaInfo.getReplicaInfo().getState()
                + " for block " + b.getBlockId());
      }
    } catch (IOException e) {
      // IOUtils.cleanup(null, ref);
      throw e;
    }

    volumeMap.add(b.getBlockPoolId(), newReplicaInfo.getReplicaInfo());
    return newReplicaInfo;
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline recoverRbw(ExtendedBlock b, long newGS,
                                                   long minBytesRcvd, long maxBytesRcvd) throws IOException {
    LOG.info("Recover RBW replica " + b);

    ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());

    // check the replica's state
    if (replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
    }
    ReplicaBeingWritten rbw = (ReplicaBeingWritten)replicaInfo;

    LOG.info("Recovering " + rbw);

    // Stop the previous writer
    rbw.stopWriter(datanode.getDnConf().getXceiverStopTimeout());
    rbw.setWriter(Thread.currentThread());

    // check generation stamp
    long replicaGenerationStamp = rbw.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b +
              ". Expected GS range is [" + b.getGenerationStamp() + ", " +
              newGS + "].");
    }

    // check replica length
    long bytesAcked = rbw.getBytesAcked();
    long numBytes = rbw.getNumBytes();
    if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd){
      throw new ReplicaNotFoundException("Unmatched length replica " +
          replicaInfo + ": BytesAcked = " + bytesAcked +
          " BytesRcvd = " + numBytes + " are not in the range of [" +
          minBytesRcvd + ", " + maxBytesRcvd + "].");
    }

    // Truncate the potentially corrupt portion.
    // If the source was client and the last node in the pipeline was lost,
    // any corrupt data written after the acked length can go unnoticed.
    if (numBytes > bytesAcked) {
      final File replicafile = rbw.getBlockFile();
      truncateBlock(replicafile, rbw.getMetaFile(), numBytes, bytesAcked);
      rbw.setLastChecksumAndDataLen(bytesAcked, null);
    }

    // bump the replica's generation stamp to newGS
    replicaInfo.bumpReplicaGS(newGS);

    return rbw;
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline convertTemporaryToRbw(
      final ExtendedBlock b) throws IOException {
    final long blockId = b.getBlockId();
    final long expectedGs = b.getGenerationStamp();
    final long visible = b.getNumBytes();
    LOG.info(
        "Convert " + b + " from Temporary to RBW, visible length=" + visible);

    final ReplicaInfo temp;
    {
      // get replica
      final ReplicaInfo r = volumeMap.get(b.getBlockPoolId(), blockId);
      if (r == null) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
      }
      // check the replica's state
      if (r.getState() != ReplicaState.TEMPORARY) {
        throw new ReplicaAlreadyExistsException(
            "r.getState() != ReplicaState.TEMPORARY, r=" + r);
      }
      temp = r;
    }
    // check generation stamp
    if (temp.getGenerationStamp() != expectedGs) {
      throw new ReplicaAlreadyExistsException(
          "temp.getGenerationStamp() != expectedGs = " + expectedGs +
              ", temp=" + temp);
    }

    // TODO: check writer?
    // set writer to the current thread
    // temp.setWriter(Thread.currentThread());

    // check length
    final long numBytes = temp.getNumBytes();
    if (numBytes < visible) {
      throw new IOException(
          numBytes + " = numBytes < visible = " + visible + ", temp=" + temp);
    }
    // check volume
    final FsVolumeImpl v = (FsVolumeImpl) temp.getVolume();
    if (v == null) {
      throw new IOException("r.getVolume() = null, temp=" + temp);
    }

    final ReplicaInPipeline rbw = v.convertTemporaryToRbw(b, temp);

    if(rbw.getState() != ReplicaState.RBW) {
      throw new IOException("Expected replica state: " + ReplicaState.RBW
              + " obtained " + rbw.getState() + " for converting block "
              + b.getBlockId());
    }
    // overwrite the RBW in the volume map
    volumeMap.add(b.getBlockPoolId(), rbw.getReplicaInfo());
    return rbw;
  }

  private boolean isReplicaProvided(ReplicaInfo replicaInfo) {
    if (replicaInfo == null) {
      return false;
    }
    return replicaInfo.getVolume().getStorageType() == StorageType.PROVIDED;
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaInPipeline createTemporary(StorageType storageType, ExtendedBlock b)
      throws IOException {
    ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), b.getBlockId());
    if (replicaInfo != null) {
      throw new ReplicaAlreadyExistsException("Block " + b +
          " already exists in state " + replicaInfo.getState() +
          " and thus cannot be created.");
    }

    FsVolumeImpl v = volumes.getNextVolume(storageType, b.getNumBytes());
    // create a temporary file to hold block in the designated volume
    File f = v.createTmpFile(b.getBlockPoolId(), b.getLocalBlock());

    LocalReplicaInPipeline newReplicaInfo =
            new ReplicaBuilder(ReplicaState.TEMPORARY)
                    .setBlockId(b.getBlockId())
                    .setGenerationStamp(b.getGenerationStamp())
                    .setDirectoryToUse(f.getParentFile())
                    .setBytesToReserve(b.getLocalBlock().getNumBytes())
                    .setFsVolume(v)
                    .buildLocalReplicaInPipeline();

    volumeMap.add(b.getBlockPoolId(), newReplicaInfo);

    return newReplicaInfo;
  }

  /**
   * Sets the offset in the meta file so that the
   * last checksum will be overwritten.
   */
  @Override // FsDatasetSpi
  public void adjustCrcChannelPosition(ExtendedBlock b,
      ReplicaOutputStreams streams, int checksumSize) throws IOException {
    FileOutputStream file = (FileOutputStream) streams.getChecksumOut();
    FileChannel channel = file.getChannel();
    long oldPos = channel.position();
    long newPos = oldPos - checksumSize;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing meta file offset of block " + b + " from " +
          oldPos + " to " + newPos);
    }
    channel.position(newPos);
  }

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  @Override // FsDatasetSpi
  public synchronized void finalizeBlock(ExtendedBlock b) throws IOException {
    if (Thread.interrupted()) {
      // Don't allow data modifications from interrupted threads
      throw new IOException("Cannot finalize block from Interrupted Thread");
    }
    ReplicaInfo replicaInfo = getReplicaInfo(b);
    if (replicaInfo.getState() == ReplicaState.FINALIZED) {
      // this is legal, when recovery happens on a file that has
      // been opened for append but never modified
      return;
    }
    finalizeReplica(b.getBlockPoolId(), replicaInfo);
  }

  private synchronized ReplicaInfo finalizeReplica(String bpid,
      ReplicaInfo replicaInfo) throws IOException {
    ReplicaInfo newReplicaInfo;
    if (replicaInfo.getState() == ReplicaState.RUR &&
          replicaInfo.getOriginalReplica().getState()
          == ReplicaState.FINALIZED) {
        newReplicaInfo = replicaInfo.getOriginalReplica();
    } else {
      FsVolumeImpl v = (FsVolumeImpl) replicaInfo.getVolume();
      if (v == null) {
          throw new IOException("No volume for block " + replicaInfo);
      }

      newReplicaInfo = v.addFinalizedBlock(
              bpid, replicaInfo, replicaInfo, replicaInfo.getBytesReserved());
      }
    assert newReplicaInfo.getState() == ReplicaState.FINALIZED
        : "Replica should be finalized";
    volumeMap.add(bpid, newReplicaInfo);
    return newReplicaInfo;
  }

  /**
   * Remove the temporary block file (if any)
   */
  @Override // FsDatasetSpi
  public synchronized void unfinalizeBlock(ExtendedBlock b) throws IOException {
    ReplicaInfo replicaInfo =
        volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    if (replicaInfo != null &&
        replicaInfo.getState() == ReplicaState.TEMPORARY) {
      // remove from volumeMap
      volumeMap.remove(b.getBlockPoolId(), b.getLocalBlock());

      // delete the on-disk temp file
        if (delBlockFromDisk(replicaInfo)) {
        LOG.warn("Block " + b + " unfinalized and removed. ");
      }
    }
  }

  /**
   * Remove a block from disk
   * @param info the replica that needs to be deleted
   * @return true if data for the replica are deleted; false otherwise
   */
  private boolean delBlockFromDisk(ReplicaInfo info) {

    if (!info.deleteBlockData()) {
      LOG.warn("Not able to delete the block data for replica " + info);
      return false;
    } else { // remove the meta file
      if (!info.deleteMetadata()) {
        LOG.warn("Not able to delete the meta data for replica " + info);
        return false;
      }
    }
    return true;
  }

  /**
   * Generates a block report from the in-memory block map.
   */
  @Override // FsDatasetSpi
  public Map<DatanodeStorage, BlockReport> getBlockReports(String bpid) {
    Map<DatanodeStorage, BlockReport> blockReportsMap =
        new HashMap<DatanodeStorage, BlockReport>();

    Map<String, BlockReport.Builder> builders =
        new HashMap<String, BlockReport.Builder>();

    List<FsVolumeImpl> curVolumes = getVolumes();
    for (FsVolumeSpi v : curVolumes) {
      builders.put(v.getStorageID(), BlockReport.builder(NUM_BUCKETS));
    }

    synchronized(this) {
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        // skip PROVIDED replicas.
        if (b.getVolume().getStorageType() == StorageType.PROVIDED) {
          continue;
        }
        switch(b.getState()) {
          case FINALIZED:
          case RBW:
          case RWR:
            builders.get(b.getVolume().getStorageID()).add(b);
            break;
          case RUR:
            ReplicaUnderRecovery rur = (ReplicaUnderRecovery) b;
            builders.get(b.getVolume().getStorageID()).add(rur.getOriginalReplica());
            break;
          case TEMPORARY:
            break;
          default:
            assert false : "Illegal ReplicaInfo state.";
        }
      }
    }

    for (FsVolumeImpl v : curVolumes) {
      blockReportsMap.put(v.toDatanodeStorage(),builders.get(v.getStorageID()).build());
    }

    return blockReportsMap;
  }

  /**
   * Get the list of finalized blocks from in-memory blockmap for a block pool.
   */
  @Override
  public synchronized List<ReplicaInfo> getFinalizedBlocks(String bpid) {
    ArrayList<ReplicaInfo> finalized =
        new ArrayList<>(volumeMap.size(bpid));

    Collection<ReplicaInfo> replicas = volumeMap.replicas(bpid);
    if (replicas != null) {
      for (ReplicaInfo b : replicas) {
        if (b.getState() == ReplicaState.FINALIZED) {
          finalized.add(b);
        }
      }
    }
    return finalized;
  }

  /**
   * Check if a block is valid.
   *
   * @param b           The block to check.
   * @param minLength   The minimum length that the block must have.  May be 0.
   * @param state       If this is null, it is ignored.  If it is non-null, we
   *                        will check that the replica has this state.
   *
   * @throws ReplicaNotFoundException          If the replica is not found
   *
   * @throws UnexpectedReplicaStateException   If the replica is not in the
   *                                             expected state.
   * @throws FileNotFoundException             If the block file is not found or there
   *                                              was an error locating it.
   * @throws EOFException                      If the replica length is too short.
   *
   * @throws IOException                       May be thrown from the methods called.
   */
  @Override // FsDatasetSpi
  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
          throws ReplicaNotFoundException, FileNotFoundException, EOFException, IOException {
    final ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
            b.getLocalBlock());
    if (replicaInfo == null) {
      throw new ReplicaNotFoundException(b);
    }
    if (!replicaInfo.blockDataExists()) {
      throw new FileNotFoundException(replicaInfo.getBlockURI().toString());
    }
    long onDiskLength = getLength(b);
    if (onDiskLength < minLength) {
      throw new EOFException(b + "'s on-disk length " + onDiskLength
              + " is shorter than minLength " + minLength);
    }
  }

  /**
   * Check whether the given block is a valid one.
   * valid means finalized
   */
  @Override // FsDatasetSpi
  public boolean isValidBlock(ExtendedBlock b) {
    return isValid(b, ReplicaState.FINALIZED);
  }

  /**
   * Check whether the given block is a valid RBW.
   */
  @Override // {@link FsDatasetSpi}
  public boolean isValidRbw(final ExtendedBlock b) {
    return isValid(b, ReplicaState.RBW);
  }

  /** Does the block exist and have the given state? */
  private boolean isValid(final ExtendedBlock b, final ReplicaState state) {
    try {
      checkBlock(b, 0, state);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  ReplicaInfo validateBlockFile(String bpid, long blockId) {
    //Should we check for metadata file too?
    final ReplicaInfo r;
    synchronized (this) {
      r = volumeMap.get(bpid, blockId);
    }

    if (r != null) {
      if (r.blockDataExists()) {
        return r;
      }

      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskError();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("blockId=" + blockId + ", replica=" + r);
    }
    return null;
  }

  /**
   * Check the files of a replica.
   */
  static void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    //check replica's data exists
    if (!r.blockDataExists()) {
      throw new FileNotFoundException("Block data not found, r=" + r);
    }
    if (r.getBytesOnDisk() != r.getBlockDataLength()) {
      throw new IOException("Block length mismatch, len="
          + r.getBlockDataLength() + " but r=" + r);
    }

    //check replica's meta file
    if (!r.metadataExists()) {
      throw new IOException(r.getMetadataURI() + " does not exist, r=" + r);
    }
    if (r.getMetadataLength() == 0) {
      throw new IOException("Metafile is empty, r=" + r);
    }
  }

  /**
   * We're informed that a block is no longer valid.  We
   * could lazily garbage-collect the block, but why bother?
   * just get rid of it.
   */
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (Block invalidBlk : invalidBlks) {
      final ReplicaInfo removing;
      final FsVolumeImpl v;
      synchronized (this) {
        // f = getFile(bpid, invalidBlk.getBlockId());
        ReplicaInfo info = volumeMap.get(bpid, invalidBlk);
        if (info == null) {
          ReplicaInfo infoByBlockId =
                  volumeMap.get(bpid, invalidBlk.getBlockId());
          if (infoByBlockId == null) {
            // It is okay if the block is not found -- it
            // may be deleted earlier.
            LOG.info("Failed to delete replica " + invalidBlk
                    + ": ReplicaInfo not found.");
          } else {
            errors.add("Failed to delete replica " + invalidBlk
                    + ": GenerationStamp not matched, existing replica is "
                    + Block.toString(infoByBlockId));
          }
          continue;
        }
        if (info.getGenerationStamp() != invalidBlk.getGenerationStamp()) {
          errors.add("Failed to delete replica " + invalidBlk +
              ": GenerationStamp not matched, info=" + info);
          continue;
        }
        v = (FsVolumeImpl) info.getVolume();
        if (v == null) {
          errors.add("Failed to delete replica " + invalidBlk
              +  ". No volume for replica " + info);
          continue;
        }
        try {
          File blockFile = new File(info.getBlockURI());
          if (blockFile != null && blockFile.getParentFile() == null) {
            errors.add("Failed to delete replica " + invalidBlk
                    +  ". Parent not found for block file: " + blockFile);
            continue;
          }
        } catch(IllegalArgumentException e) {
          LOG.warn("Parent directory check failed; replica " + info
                  + " is not backed by a local file");
        }
          /*
        ReplicaState replicaState = info.getState();
        if (replicaState == ReplicaState.FINALIZED ||
                (replicaState == ReplicaState.RUR &&
                        ((ReplicaUnderRecovery) info).getOriginalReplica().getState() ==
                                ReplicaState.FINALIZED)) {
            v.clearPath(bpid, parent);
          }
        }
        */
        removing = volumeMap.remove(bpid, invalidBlk);
        // addDeletingBlock(bpid, removing.getBlockId());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Block file " + removing.getBlockURI()
                  + " is to be deleted");
        }
        if (removing instanceof ReplicaInPipeline) {
          ((ReplicaInPipeline) removing).releaseAllBytesReserved();
        }
      }

      // Delete the block asynchronously to make sure we can do it fast enough
      asyncDiskService.deleteAsync(v, removing,
              new ExtendedBlock(bpid, invalidBlk));
    }
    if (!errors.isEmpty()) {
      StringBuilder b = new StringBuilder("Failed to delete ")
              .append(errors.size()).append(" (out of ").append(invalidBlks.length)
              .append(") replica(s):");
      for(int i = 0; i < errors.size(); i++) {
        b.append("\n").append(i).append(") ").append(errors.get(i));
      }
      throw new IOException(b.toString());
    }
  }

  @Override // FsDatasetSpi
  public synchronized boolean contains(final ExtendedBlock block) {
    final long blockId = block.getLocalBlock().getBlockId();
    final String bpid = block.getBlockPoolId();
    final ReplicaInfo r = volumeMap.get(bpid, blockId);
    return (r != null && r.blockDataExists());
  }

  /**
   * check if a data directory is healthy
   * if some volumes failed - make sure to remove all the blocks that belong
   * to these volumes
   *
   * @throws DiskErrorException
   */
  @Override // FsDatasetSpi
  public void checkDataDir() throws DiskErrorException {
    long totalBlocks = 0, removedBlocks = 0;
    List<FsVolumeImpl> failedVols = volumes.checkDirs();

    // TODO: cross check with volumes.checkDirs()
    // If there no failed volumes return
    if (failedVols == null) {
      return;
    }
    StringBuilder sb = new StringBuilder();
    // Otherwise remove blocks for the failed volumes
    long mlsec = Time.now();
    synchronized (this) {
      for (FsVolumeImpl fv : failedVols) {
        for (String bpid : fv.getBlockPoolList()) {
          Iterator<ReplicaInfo> ib = volumeMap.replicas(bpid).iterator();
          while (ib.hasNext()) {
            ReplicaInfo b = ib.next();
            totalBlocks++;
            // check if the volume block belongs to still valid
            if (b.getVolume() == fv) {
              LOG.warn("Removing replica " + bpid + ":" + b.getBlockId() +
                  " on failed volume " + fv.getBaseURI());
              // report the error
              sb.append(fv.getBaseURI() + ";");
              ib.remove();
              removedBlocks++;
            }
          }
        }
      }
    } // end of sync
    mlsec = Time.now() - mlsec;
    LOG.warn("Removed " + removedBlocks + " out of " + totalBlocks +
        "(took " + mlsec + " millisecs)");

    throw new DiskErrorException("DataNode failed volumes:" + sb);
  }


  @Override // FsDatasetSpi
  public String toString() {
    return "FSDataset{dirpath='" + volumes + "'}";
  }

  private ObjectName mbeanName;
  
  /**
   * Register the FSDataset MBean using the name
   * "hadoop:service=DataNode,name=FSDatasetState-<datanodeUuid>"
   */
  void registerMBean(final String datanodeUuid) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    try {
      StandardMBean bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-" + datanodeUuid, bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering FSDatasetState MBean", e);
    }
    LOG.info("Registered FSDatasetState MBean");
  }

  @Override // FsDatasetSpi
  public void shutdown() {
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
    }
    
    if (asyncDiskService != null) {
      asyncDiskService.shutdown();
    }
    
    if (volumes != null) {
      volumes.shutdown();
    }
  }

  @Override // FSDatasetMBean
  public String getStorageInfo() {
    return toString();
  }

  /**
   * Reconcile the difference between blocks on the disk and blocks in
   * volumeMap
   * <p/>
   * Check the given block for inconsistencies. Look at the
   * current state of the block and reconcile the differences as follows:
   * <ul>
   * <li>If the block file is missing, delete the block from volumeMap</li>
   * <li>If the block file exists and the block is missing in volumeMap,
   * add the block to volumeMap <li>
   * <li>If generation stamp does not match, then update the block with right
   * generation stamp</li>
   * <li>If the block length in memory does not match the actual block file length
   * then mark the block as corrupt and update the block length in memory</li>
   * <li>If the file in {@link ReplicaInfo} does not match the file on
   * the disk, update {@link ReplicaInfo} with the correct file</li>
   * </ul>
   *
   */
  @Override
  public void checkAndUpdate(String bpid, FsVolumeSpi.ScanInfo scanInfo) throws IOException {

    long blockId = scanInfo.getBlockId();
    File diskFile = scanInfo.getBlockFile();
    File diskMetaFile = scanInfo.getMetaFile();
    FsVolumeSpi vol = scanInfo.getVolume();

    Block corruptBlock = null;
    ReplicaInfo memBlockInfo;
    synchronized (this) {
      memBlockInfo = volumeMap.get(bpid, blockId);
      if (memBlockInfo != null &&
          memBlockInfo.getState() != ReplicaState.FINALIZED) {
        // Block is not finalized - ignore the difference
        return;
      }

      final FileIoProvider fileIoProvider = datanode.getFileIoProvider();
      final boolean diskMetaFileExists = diskMetaFile != null &&
          fileIoProvider.exists(vol, diskMetaFile);
      final boolean diskFileExists = diskFile != null &&
          fileIoProvider.exists(vol, diskFile);

      final long diskGS = diskMetaFileExists ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
          GenerationStamp.GRANDFATHER_GENERATION_STAMP;

      if (vol.getStorageType() == StorageType.PROVIDED) {
        if (memBlockInfo == null) {
          // replica exists on provided store but not in memory
          ReplicaInfo diskBlockInfo =
                  new ReplicaBuilder(ReplicaState.FINALIZED)
                          .setFileRegion(scanInfo.getFileRegion())
                          .setFsVolume(vol)
                          .setConf(conf)
                          .build();

          volumeMap.add(bpid, diskBlockInfo);
          LOG.warn("Added missing block to memory " + diskBlockInfo);
        } else {
          // replica exists in memory but not in the provided store
          volumeMap.remove(bpid, blockId);
          LOG.warn("Deleting missing provided block " + memBlockInfo);
        }
        return;
      }

      if (diskFile == null || !diskFile.exists()) {
        if (memBlockInfo == null) {
          // Block file does not exist and block does not exist in memory
          // If metadata file exists then delete it
          if (diskMetaFile != null && diskMetaFile.exists() &&
              diskMetaFile.delete()) {
            LOG.warn("Deleted a metadata file without a block " +
                diskMetaFile.getAbsolutePath());
          }
          return;
        }
        if (!memBlockInfo.blockDataExists()) {
          // Block is in memory and not on the disk
          // Remove the block from volumeMap
          volumeMap.remove(bpid, blockId);
          final DataBlockScanner blockScanner = datanode.getBlockScanner();
          if (blockScanner != null) {
            blockScanner.deleteBlock(bpid, new Block(blockId));
          }
          LOG.warn("Removed block " + blockId +
              " from memory with missing block file on the disk");
          // Finally remove the metadata file
          if (diskMetaFile != null && diskMetaFile.exists() &&
              diskMetaFile.delete()) {
            LOG.warn("Deleted a metadata file for the deleted block " +
                diskMetaFile.getAbsolutePath());
          }
        }
        return;
      }
      /*
       * Block file exists on the disk
       */
      if (memBlockInfo == null) {
        // Block is missing in memory - add the block to volumeMap
        ReplicaInfo diskBlockInfo = new ReplicaBuilder(ReplicaState.FINALIZED)
                .setBlockId(blockId)
                .setLength(diskFile.length())
                .setGenerationStamp(diskGS)
                .setFsVolume(vol)
                .setDirectoryToUse(diskFile.getParentFile())
                .build();
        volumeMap.add(bpid, diskBlockInfo);
        final DataBlockScanner blockScanner = datanode.getBlockScanner();
        if (blockScanner != null) {
          blockScanner.addBlock(new ExtendedBlock(bpid, diskBlockInfo));
        }
        LOG.warn("Added missing block to memory " + diskBlockInfo);
        return;
      }
      /*
       * Block exists in volumeMap and the block file exists on the disk
       */
      // Compare block files
      if (memBlockInfo.blockDataExists()) {
        if (memBlockInfo.getBlockURI().compareTo(diskFile.toURI()) != 0) {
          if (diskMetaFileExists) {
            if (memBlockInfo.metadataExists()) {
              // We have two sets of block+meta files. Decide which one to
              // keep.
              ReplicaInfo diskBlockInfo =
                  new ReplicaBuilder(ReplicaState.FINALIZED)
                    .setBlockId(blockId)
                    .setLength(diskFile.length())
                    .setGenerationStamp(diskGS)
                    .setFsVolume(vol)
                    .setDirectoryToUse(diskFile.getParentFile())
                    .build();
              ((FsVolumeImpl) vol).resolveDuplicateReplicas(bpid,
                  memBlockInfo, diskBlockInfo, volumeMap);
            }
          } else {
            if (!fileIoProvider.delete(vol, diskFile)) {
              LOG.warn("Failed to delete " + diskFile);
            }
          }
        }
      } else {
        // Block refers to a block file that does not exist.
        // Update the block with the file found on the disk. Since the block
        // file and metadata file are found as a pair on the disk, update
        // the block based on the metadata file found on the disk
        LOG.warn("Block file in replica "
            + memBlockInfo.getBlockURI()
            + " does not exist. Updating it to the file found during scan "
            + diskFile.getAbsolutePath());
        memBlockInfo.updateWithReplica(
            StorageLocation.parse(diskFile.toString()));

        LOG.warn("Updating generation stamp for block " + blockId + " from " +
            memBlockInfo.getGenerationStamp() + " to " + diskGS);
        memBlockInfo.setGenerationStampNoPersistance(diskGS);
      }

      // Compare generation stamp
      if (memBlockInfo.getGenerationStamp() != diskGS) {
        File memMetaFile = FsDatasetUtil
            .getMetaFile(diskFile, memBlockInfo.getGenerationStamp());
        if (fileIoProvider.exists(vol, memMetaFile)) {
          String warningPrefix = "Metadata file in memory "
              + memMetaFile.getAbsolutePath()
              + " does not match file found by scan ";
          if (!diskMetaFileExists) {
            LOG.warn(warningPrefix + "null");
          } else if (memMetaFile.compareTo(diskMetaFile) != 0) {
            LOG.warn(warningPrefix + diskMetaFile.getAbsolutePath());
          }
        } else {
          // Metadata file corresponding to block in memory is missing
          // If metadata file found during the scan is on the same directory
          // as the block file, then use the generation stamp from it
          try {
            File memFile = new File(memBlockInfo.getBlockURI());
            long gs = diskMetaFileExists &&
                diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
                : HdfsConstants.GRANDFATHER_GENERATION_STAMP;

          LOG.warn("Updating generation stamp for block " + blockId + " from " +
              memBlockInfo.getGenerationStamp() + " to " + gs);

            memBlockInfo.setGenerationStampNoPersistance(gs);
          } catch (IllegalArgumentException e) {
            //exception arises because the URI cannot be converted to a file
            LOG.warn("Block URI could not be resolved to a file", e);
          }
        }
      }

      // Compare block size
      if (memBlockInfo.getNumBytes() != memBlockInfo.getBlockDataLength()) {
        // Update the length based on the block file
        corruptBlock = new Block(memBlockInfo);
        LOG.warn("Updating size of block " + blockId + " from "
            + memBlockInfo.getNumBytes() + " to "
            + memBlockInfo.getBlockDataLength());
        memBlockInfo.setNumBytes(memBlockInfo.getBlockDataLength());
      }
    }

    // Send corrupt block report outside the lock
    if (corruptBlock != null) {
      LOG.warn("Reporting the block " + corruptBlock +
          " as corrupt due to length mismatch");
      try {
        datanode.reportBadBlocks(new ExtendedBlock(bpid, corruptBlock));
      } catch (IOException e) {
        LOG.warn("Failed to repot bad block " + corruptBlock, e);
      }
    }
  }


  /**
   * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
   */
  @Override // FsDatasetSpi
  @Deprecated
  public ReplicaInfo getReplica(String bpid, long blockId) {
    return volumeMap.get(bpid, blockId);
  }

  @Override
  public synchronized String getReplicaString(String bpid, long blockId) {
    final Replica r = volumeMap.get(bpid, blockId);
    return r == null ? "null" : r.toString();
  }

  @Override // FsDatasetSpi
  public synchronized ReplicaRecoveryInfo initReplicaRecovery(
      RecoveringBlock rBlock) throws IOException {
    return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(), volumeMap,
        rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp());
  }

  /**
   * static version of {@link #initReplicaRecovery(RecoveringBlock)}.
   */
  static ReplicaRecoveryInfo initReplicaRecovery(String bpid, ReplicaMap map,
                                          Block block, long recoveryId) throws IOException {
    final ReplicaInfo replica = map.get(bpid, block.getBlockId());
    LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId +
        ", replica=" + replica);

    //check replica
    if (replica == null) {
      return null;
    }

    //stop writer if there is any
    if (replica instanceof ReplicaInPipeline) {
      final ReplicaInPipeline rip = (ReplicaInPipeline) replica;
      rip.stopWriter(XCEIVER_STOP_TIMEOUT);

      //check replica bytes on disk.
      if (rip.getBytesOnDisk() < rip.getVisibleLength()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:" +
            " getBytesOnDisk() < getVisibleLength(), rip=" + rip);
      }

      //check the replica's files
      checkReplicaFiles(replica);
    }

    //check generation stamp
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block=" +
              block + ", replica=" + replica);
    }

    //check recovery id
    if (replica.getGenerationStamp() >= recoveryId) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:" +
          " replica.getGenerationStamp() >= recoveryId = " + recoveryId +
          ", block=" + block + ", replica=" + replica);
    }

    //check RUR
    final ReplicaInfo rur;
    if (replica.getState() == ReplicaState.RUR) {
      rur = replica;
      if (rur.getRecoveryID() >= recoveryId) {
        throw new RecoveryInProgressException(
            "rur.getRecoveryID() >= recoveryId = " + recoveryId
            + ", block=" + block + ", rur=" + rur);
      }
      final long oldRecoveryID = rur.getRecoveryID();
      rur.setRecoveryID(recoveryId);
      LOG.info("initReplicaRecovery: update recovery id for " + block
          + " from " + oldRecoveryID + " to " + recoveryId);
    }
    else {
      rur = new ReplicaBuilder(ReplicaState.RUR)
          .from(replica).setRecoveryId(recoveryId).build();
      map.add(bpid, rur);
      LOG.info("initReplicaRecovery: changing replica state for " + block +
          " from " + replica.getState() + " to " + rur.getState());
      if (replica.getState() == ReplicaState.TEMPORARY || replica
          .getState() == ReplicaState.RBW) {
        ((ReplicaInPipeline) replica).releaseAllBytesReserved();
      }
    }
    return rur.createInfo();
  }

  @Override // FsDatasetSpi
  public synchronized String updateReplicaUnderRecovery(
      final ExtendedBlock oldBlock, final long recoveryId, final long newlength)
      throws IOException {
    //get replica
    final String bpid = oldBlock.getBlockPoolId();
    final ReplicaInfo replica = volumeMap.get(bpid, oldBlock.getBlockId());
    LOG.info("updateReplica: " + oldBlock + ", recoveryId=" + recoveryId +
        ", length=" + newlength + ", replica=" + replica);

    //check replica
    if (replica == null) {
      throw new ReplicaNotFoundException(oldBlock);
    }

    //check replica state
    if (replica.getState() != ReplicaState.RUR) {
      throw new IOException(
          "replica.getState() != " + ReplicaState.RUR + ", replica=" + replica);
    }

    //check replica's byte on disk
    if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:" +
          " replica.getBytesOnDisk() != block.getNumBytes(), block=" +
          oldBlock + ", replica=" + replica);
    }

    //check replica files before update
    checkReplicaFiles(replica);

    //update replica
    final ReplicaInfo finalized =
        updateReplicaUnderRecovery(oldBlock.getBlockPoolId(),
            (ReplicaUnderRecovery) replica, recoveryId, newlength);
    assert finalized.getBlockId() == oldBlock.getBlockId() &&
        finalized.getGenerationStamp() == recoveryId &&
        finalized.getNumBytes() == newlength :
        "Replica information mismatched: oldBlock=" + oldBlock +
            ", recoveryId=" + recoveryId + ", newlength=" + newlength +
            ", finalized=" + finalized;

    //check replica files after update
    checkReplicaFiles(finalized);

    //return storage ID
    return getVolume(new ExtendedBlock(bpid, finalized)).getStorageID();
  }

  private ReplicaInfo updateReplicaUnderRecovery(String bpid,
      ReplicaUnderRecovery rur, long recoveryId, long newlength)
      throws IOException {
    //check recovery id
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException(
          "rur.getRecoveryID() != recoveryId = " + recoveryId + ", rur=" + rur);
    }

    // bump rur's GS to be recovery id
    rur.bumpReplicaGS(recoveryId);

    //update length
    final File replicafile = rur.getBlockFile();
    if (rur.getNumBytes() < newlength) {
      throw new IOException(
          "rur.getNumBytes() < newlength = " + newlength + ", rur=" + rur);
    }
    if (rur.getNumBytes() > newlength) {
      //rur.unlinkBlock(1);
      rur.breakHardLinksIfNeeded();
      rur.truncateBlock(newlength);
      // update RUR with the new length
      rur.setNumBytesNoPersistance(newlength);
    }

    // finalize the block
    return finalizeReplica(bpid, rur);
  }

  @Override // FsDatasetSpi
  public synchronized long getReplicaVisibleLength(final ExtendedBlock block)
      throws IOException {
    final Replica replica =
        getReplicaInfo(block.getBlockPoolId(), block.getBlockId());
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block=" +
              block + ", replica=" + replica);
    }
    return replica.getVisibleLength();
  }
  
  @Override
  public synchronized void addBlockPool(String bpid, Configuration conf)
      throws IOException {
    LOG.info("Adding block pool " + bpid);
    volumes.addBlockPool(bpid, conf);
    volumeMap.initBlockPool(bpid);
    volumes.getVolumeMap(bpid, volumeMap);
  }

  @Override
  public synchronized void shutdownBlockPool(String bpid) {
    LOG.info("Removing block pool " + bpid);
    volumeMap.cleanUpBlockPool(bpid);
    volumes.removeBlockPool(bpid);
  }



/**
   * Class for representing the Datanode volume information
   */
  private static class VolumeInfo {
    final String directory;
    final long usedSpace; // size of space used by HDFS
    final long freeSpace; // size of free space excluding reserved space
    final long reservedSpace; // size of space reserved for non-HDFS
    final long reservedSpaceForReplicas; // size of space reserved RBW or
                                    // re-replication
    final long numBlocks;
    final StorageType storageType;

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
      this.reservedSpaceForReplicas = v.getReservedForReplicas();
      this.numBlocks = v.getNumBlocks();
      this.storageType = v.getStorageType();
    }
  }

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<>();
    for (FsVolumeImpl volume : volumes.getVolumes()) {
      long used = 0;
      long free = 0;
      try {
        used = volume.getDfsUsed();
        free = volume.getAvailable();
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        used = 0;
        free = 0;
      }
      
      info.add(new VolumeInfo(volume, used, free));
    }
    return info;
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    final Map<String, Object> info = new HashMap<>();
    Collection<VolumeInfo> volumes = getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      innerInfo.put("reservedSpaceForReplicas", v.reservedSpaceForReplicas);
      innerInfo.put("numBlocks", v.numBlocks);
      innerInfo.put("storageType", v.storageType);
      info.put(v.directory, innerInfo);
    }
    return info;
  }

  @Override //FsDatasetSpi
  public synchronized void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    if (!force) {
      for (FsVolumeImpl volume : volumes.getVolumes()) {
        if (!volume.isBPDirEmpty(bpid)) {
          LOG.warn(bpid + " has some block files, cannot delete unless forced");
          throw new IOException(
              "Cannot delete block pool, " + "it contains some block files");
        }
      }
    }
    for (FsVolumeImpl volume : volumes.getVolumes()) {
      volume.deleteBPDirectories(bpid, force);
    }
  }
  
  @Override // FsDatasetSpi
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
  final Replica replica = volumeMap.get(block.getBlockPoolId(),
          block.getBlockId());
  if (replica == null) {
    throw new ReplicaNotFoundException(block);
  }
  if (replica.getGenerationStamp() < block.getGenerationStamp()) {
    throw new IOException(
            "Replica generation stamp < block generation stamp, block="
                    + block + ", replica=" + replica);
  } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
    block.setGenerationStamp(replica.getGenerationStamp());
  }

  ReplicaInfo r = getBlockReplica(block);
  File blockFile = new File(r.getBlockURI());
  File metaFile = new File(r.getMetadataURI());
  BlockLocalPathInfo info = new BlockLocalPathInfo(block,
          blockFile.getAbsolutePath(), metaFile.toString());
  return info;
  }

  @Override // FsDatasetSpi
  public HdfsBlocksMetadata getHdfsBlocksMetadata(List<ExtendedBlock> blocks)
      throws IOException {
    // List of VolumeIds, one per volume on the datanode
    List<byte[]> blocksVolumeIds =
        new ArrayList<>(volumes.getVolumes().size());
    // List of indexes into the list of VolumeIds, pointing at the VolumeId of
    // the volume that the block is on
    List<Integer> blocksVolumeIndexes = new ArrayList<>(blocks.size());
    // Initialize the list of VolumeIds simply by enumerating the volumes
    for (int i = 0; i < volumes.getVolumes().size(); i++) {
      blocksVolumeIds.add(ByteBuffer.allocate(4).putInt(i).array());
    }
    // Determine the index of the VolumeId of each block's volume, by comparing
    // the block's volume against the enumerated volumes
    for (ExtendedBlock block : blocks) {
      FsVolumeSpi blockVolume = getReplicaInfo(block).getVolume();
      boolean isValid = false;
      int volumeIndex = 0;
      for (FsVolumeImpl volume : volumes.getVolumes()) {
        // This comparison of references should be safe
        if (blockVolume == volume) {
          isValid = true;
          break;
        }
        volumeIndex++;
      }
      // Indicates that the block is not present, or not found in a data dir
      if (!isValid) {
        volumeIndex = Integer.MAX_VALUE;
      }
      blocksVolumeIndexes.add(volumeIndex);
    }
    return new HdfsBlocksMetadata(blocks.toArray(new ExtendedBlock[]{}),
        blocksVolumeIds, blocksVolumeIndexes);
  }

  @Override
  public RollingLogs createRollingLogs(String bpid, String prefix)
            throws IOException {
      String dir = null;
      final List<FsVolumeImpl> volumes = getVolumes();
      for (FsVolumeImpl vol : volumes) {
        if(vol.getCurrentDir() == null) { // GABRIEL - provided vol's dir is always null
          LOG.error("Current dir " + vol + " is null. Skipping it");
          break;
        }
        String bpDir = vol.getCurrentDir().getPath() + "/" + bpid;
        if (RollingLogsImpl.isFilePresent(bpDir, prefix)) {
          dir = bpDir;
          break;
        }
      }
      if (dir == null && volumes.get(0).getCurrentDir() != null) {
        dir = volumes.get(0).getCurrentDir().getPath() + "/" + bpid;
      }
      return new RollingLogsImpl(dir, prefix);
    }
  }
