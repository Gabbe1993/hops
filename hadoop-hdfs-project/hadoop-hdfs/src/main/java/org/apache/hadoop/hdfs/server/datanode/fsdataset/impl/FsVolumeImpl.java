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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The underlying volume used to store replica.
 * <p/>
 * It uses the {@link FsDatasetImpl} object for synchronization.
 */
@InterfaceAudience.Private
public class FsVolumeImpl implements FsVolumeSpi {
  public static final Logger LOG =
      LoggerFactory.getLogger(FsVolumeImpl.class);
  private static final ObjectWriter WRITER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(BlockIteratorState.class);

  private final FsDatasetImpl dataset;
  private final String storageID;
  private final StorageType storageType;
  private final Map<String, BlockPoolSlice> bpSlices
      = new ConcurrentHashMap<String, BlockPoolSlice>();

  // Refers to the base StorageLocation used to construct this volume
  // (i.e., does not include STORAGE_DIR_CURRENT in
  // <location>/STORAGE_DIR_CURRENT/)
  private final StorageLocation storageLocation;
  private final File currentDir;    // <StorageDirectory>/current
  private final DF usage;
  private final long reserved;

  // Disk space reserved for blocks (RBW or Re-replicating) open for write.
  private AtomicLong reservedForReplicas;
  private long recentReserved = 0;
  private final Configuration conf;
  // Capacity configured. This is useful when we want to
  // limit the visible capacity for tests. If negative, then we just
  // query from the filesystem.
  protected volatile long configuredCapacity;
  private final FileIoProvider fileIoProvider;
  
  FsVolumeImpl(
      FsDatasetImpl dataset, String storageID, StorageDirectory sd,
      FileIoProvider fileIoProvider, Configuration conf) throws IOException {
    if (sd.getStorageLocation() == null) {
      throw new IOException("StorageLocation specified for storage directory " +
          sd + " is null");
    }
    this.dataset = dataset;
    this.storageID = storageID;
    this.reservedForReplicas = new AtomicLong(0L);
    this.storageLocation = sd.getStorageLocation();
    this.currentDir = sd.getCurrentDir();
    this.storageType = storageLocation.getStorageType();
    this.reserved = conf.getLong(DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY
        + "." + StringUtils.toLowerCase(storageType.toString()), conf.getLong(
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_KEY,
        DFSConfigKeys.DFS_DATANODE_DU_RESERVED_DEFAULT));
    this.configuredCapacity = -1;
    if (currentDir != null) {
      File parent = currentDir.getParentFile();
      this.usage = new DF(parent, conf);
    } else {
      this.usage = null;
    }
    this.conf = conf;
    this.fileIoProvider = fileIoProvider;
  }

  File getCurrentDir() {
    return currentDir;
  }

  File getRbwDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getRbwDir();
  }

  void onBlockFileDeletion(String bpid, long value) {
    decDfsUsedAndNumBlocks(bpid, value, true);
  }

  void onMetaFileDeletion(String bpid, long value) {
    decDfsUsedAndNumBlocks(bpid, value, false);
  }

  private void decDfsUsedAndNumBlocks(String bpid, long value,
                                      boolean blockFileDeleted) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.decDfsUsed(value);
      if (blockFileDeleted) {
        bp.decrNumBlocks();
      }
    }
  }

  void incDfsUsedAndNumBlocks(String bpid, long value) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.incDfsUsed(value);
      bp.incrNumBlocks();
    }
  }

  void incDfsUsed(String bpid, long value) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.incDfsUsed(value);
    }
  }

  void decDfsUsed(String bpid, long value) {
    synchronized (dataset) {
      BlockPoolSlice bp = bpSlices.get(bpid);
      if (bp != null) {
        bp.decDfsUsed(value);
      }
    }
  }

  @VisibleForTesting
  public long getDfsUsed() throws IOException {
    long dfsUsed = 0;
    synchronized (dataset) {
      for (BlockPoolSlice s : bpSlices.values()) {
        dfsUsed += s.getDfsUsed();
      }
    }
    return dfsUsed;
  }

  long getBlockPoolUsed(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getDfsUsed();
  }
  
  /**
   * Calculate the capacity of the filesystem, after removing any
   * reserved capacity.
   * @return the unreserved number of bytes left in this filesystem. May be zero.
   */
  @VisibleForTesting
  public long getCapacity() {
    if (configuredCapacity < 0) {
      long remaining = usage.getCapacity() - reserved;
      return remaining > 0 ? remaining : 0;
    }

    return configuredCapacity;
  }

  /**
   * This function MUST NOT be used outside of tests.
   *
   * @param capacity
   */
  @VisibleForTesting
  public void setCapacityForTesting(long capacity) {
    this.configuredCapacity = capacity;
  }

  /*
   * Calculate the available space of the filesystem, excluding space reserved
   * for non-HDFS and space reserved for RBW
   *
   * @return the available number of bytes left in this filesystem. May be zero.
   */
  @Override
  public long getAvailable() throws IOException {
    long remaining = getCapacity() - getDfsUsed() - getReservedForReplicas();
    long available = usage.getAvailable()  - getRemainingReserved()
        - getReservedForReplicas();
    if (remaining > available) {
      remaining = available;
    }
    return (remaining > 0) ? remaining : 0;
  }

  long getActualNonDfsUsed() throws IOException {
    return usage.getUsed() - getDfsUsed();
  }

  private long getRemainingReserved() throws IOException {
    long actualNonDfsUsed = getActualNonDfsUsed();
    if (actualNonDfsUsed < reserved) {
      return reserved - actualNonDfsUsed;
    }
    return 0L;
  }

  /**
   * Unplanned Non-DFS usage, i.e. Extra usage beyond reserved.
   *
   * @return Disk usage excluding space used by HDFS and excluding space
   * reserved for blocks open for write.
   * @throws IOException
   */
  public long getNonDfsUsed() throws IOException {
    long actualNonDfsUsed = getActualNonDfsUsed();
    if (actualNonDfsUsed < reserved) {
      return 0L;
    }
    return actualNonDfsUsed - reserved;
  }

  @VisibleForTesting
  long getDfAvailable() {
    return usage.getAvailable();
  }

  @VisibleForTesting
  public long getReservedForReplicas() {
    return reservedForReplicas.get();
  }

  @VisibleForTesting
  long getRecentReserved() {
    return recentReserved;
  }

  long getReserved() {
    return reserved;
  }

  @VisibleForTesting
  BlockPoolSlice getBlockPoolSlice(String bpid) throws IOException {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp == null) {
      throw new IOException("block pool " + bpid + " is not found");
    }
    return bp;
  }

  @Override
  public URI getBaseURI() {
    return new File(currentDir.getParent()).toURI();
  }

  @Override
  public DF getUsageStats(Configuration conf) {
    if (currentDir != null) {
      try {
        return new DF(new File(currentDir.getParent()), conf);
      } catch (IOException e) {
        LOG.error("Unable to get disk statistics for volume " + this);
      }
    }
    return null;
  }
  @Override
  public StorageLocation getStorageLocation() {
    return storageLocation;
  }

  @VisibleForTesting
  public File getFinalizedDir(String bpid) throws IOException {
    return getBlockPoolSlice(bpid).getFinalizedDir();
  }

  /**
   * Make a deep copy of the list of currently active BPIDs
   */
  @Override
  public String[] getBlockPoolList() {
    return bpSlices.keySet().toArray(new String[bpSlices.keySet().size()]);
  }

  /**
   * Temporary files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createTmpFile(String bpid, Block b) throws IOException {
    //checkReference();
    reserveSpaceForReplica(b.getNumBytes());
    try {
    return getBlockPoolSlice(bpid).createTmpFile(b);
    } catch (IOException exception) {
      releaseReservedSpace(b.getNumBytes());
      throw exception;
    }
  }

  @Override
  public void reserveSpaceForReplica(long bytesToReserve) {
    if (bytesToReserve != 0) {
      reservedForReplicas.addAndGet(bytesToReserve);
      recentReserved = bytesToReserve;
    }
  }

  @Override
  public void releaseReservedSpace(long bytesToRelease) {
    if (bytesToRelease != 0) {

      long oldReservation, newReservation;
      do {
        oldReservation = reservedForReplicas.get();
        newReservation = oldReservation - bytesToRelease;
        if (newReservation < 0) {
          // Failsafe, this should never occur in practice, but if it does we
          // don't want to start advertising more space than we have available.
          newReservation = 0;
        }
      } while (!reservedForReplicas.compareAndSet(oldReservation,
          newReservation));
    }
  }

  @Override
  public void releaseLockedMemory(long bytesToRelease) {

  }

  public void resolveDuplicateReplicas(String bpid, ReplicaInfo memBlockInfo,
                                       ReplicaInfo diskBlockInfo, ReplicaMap volumeMap) throws IOException {
    getBlockPoolSlice(bpid).resolveDuplicateReplicas(
            memBlockInfo, diskBlockInfo, volumeMap);
  }

  private enum SubdirFilter implements FilenameFilter {
    INSTANCE;

    @Override
    public boolean accept(File dir, String name) {
      return name.startsWith("subdir");
    }
  }

  private enum BlockFileFilter implements FilenameFilter {
    INSTANCE;

    @Override
    public boolean accept(File dir, String name) {
      return !name.endsWith(".meta") &&
              name.startsWith(Block.BLOCK_FILE_PREFIX);
    }
  }

  @VisibleForTesting
  public static String nextSorted(List<String> arr, String prev) {
    int res = 0;
    if (prev != null) {
      res = Collections.binarySearch(arr, prev);
      if (res < 0) {
        res = -1 - res;
      } else {
        res++;
      }
    }
    if (res >= arr.size()) {
      return null;
    }
    return arr.get(res);
  }

  private static class BlockIteratorState {
    BlockIteratorState() {
      lastSavedMs = iterStartMs = Time.now();
      curFinalizedDir = null;
      curFinalizedSubDir = null;
      curEntry = null;
      atEnd = false;
    }

    // The wall-clock ms since the epoch at which this iterator was last saved.
    @JsonProperty
    private long lastSavedMs;

    // The wall-clock ms since the epoch at which this iterator was created.
    @JsonProperty
    private long iterStartMs;

    @JsonProperty
    private String curFinalizedDir;

    @JsonProperty
    private String curFinalizedSubDir;

    @JsonProperty
    private String curEntry;

    @JsonProperty
    private boolean atEnd;
  }

  /**
   * A BlockIterator implementation for FsVolumeImpl.
   *
   */
  // BlockIteratorImpl is currently only being used by the provided storage, see testProvidedBlockIterator
  private class BlockIteratorImpl implements FsVolumeSpi.BlockIterator {
    private final File bpidDir;
    private final String name;
    private final String bpid;
    private long maxStalenessMs = 0;

    private List<String> cache;
    private long cacheMs;

    private BlockIteratorState state;

    BlockIteratorImpl(String bpid, String name) {
      this.bpidDir = new File(currentDir, bpid);
      this.name = name;
      this.bpid = bpid;
      rewind();
    }

    /**
     * Get the next subdirectory within the block pool slice.
     *
     * @return         The next subdirectory within the block pool slice, or
     *                   null if there are no more.
     */
    private String getNextSubDir(String prev, File dir)
          throws IOException {
      List<String> children = fileIoProvider.listDirectory(
          FsVolumeImpl.this, dir, SubdirFilter.INSTANCE);
      cache = null;
      cacheMs = 0;
      if (children.size() == 0) {
        LOG.trace("getNextSubDir({}, {}): no subdirectories found in {}",
            storageID, bpid, dir.getAbsolutePath());
        return null;
      }
      Collections.sort(children);
      String nextSubDir = nextSorted(children, prev);
      if (nextSubDir == null) {
        LOG.trace("getNextSubDir({}, {}): no more subdirectories found in {}",
            storageID, bpid, dir.getAbsolutePath());
      } else {
        LOG.trace("getNextSubDir({}, {}): picking next subdirectory {} " +
            "within {}", storageID, bpid, nextSubDir, dir.getAbsolutePath());
      }
      return nextSubDir;
    }

    private String getNextFinalizedDir() throws IOException {
      File dir = Paths.get(
          bpidDir.getAbsolutePath(), "current", "finalized").toFile();
      return getNextSubDir(state.curFinalizedDir, dir);
    }

    private String getNextFinalizedSubDir() throws IOException {
      if (state.curFinalizedDir == null) {
        return null;
      }
      File dir = Paths.get(
          bpidDir.getAbsolutePath(), "current", "finalized",
              state.curFinalizedDir).toFile();
      return getNextSubDir(state.curFinalizedSubDir, dir);
    }

    private List<String> getSubdirEntries() throws IOException {
      if (state.curFinalizedSubDir == null) {
        return null; // There are no entries in the null subdir.
      }
      long now = Time.monotonicNow();
      if (cache != null) {
        long delta = now - cacheMs;
        if (delta < maxStalenessMs) {
          return cache;
        } else {
          LOG.trace("getSubdirEntries({}, {}): purging entries cache for {} " +
            "after {} ms.", storageID, bpid, state.curFinalizedSubDir, delta);
          cache = null;
        }
      }
      File dir = Paths.get(bpidDir.getAbsolutePath(), "current", "finalized",
                    state.curFinalizedDir, state.curFinalizedSubDir).toFile();
      List<String> entries = fileIoProvider.listDirectory(
          FsVolumeImpl.this, dir, BlockFileFilter.INSTANCE);
      if (entries.size() == 0) {
        entries = null;
      } else {
        Collections.sort(entries);
      }
      if (entries == null) {
        LOG.trace("getSubdirEntries({}, {}): no entries found in {}",
            storageID, bpid, dir.getAbsolutePath());
      } else {
        LOG.trace("getSubdirEntries({}, {}): listed {} entries in {}",
            storageID, bpid, entries.size(), dir.getAbsolutePath());
      }
      cache = entries;
      cacheMs = now;
      return cache;
    }

    /**
     * Get the next block.<p/>
     *
     * Each volume has a hierarchical structure.<p/>
     *
     * <code>
     * BPID B0
     *   finalized/
     *     subdir0
     *       subdir0
     *         blk_000
     *         blk_001
     *       ...
     *     subdir1
     *       subdir0
     *         ...
     *   rbw/
     * </code>
     *
     * When we run out of entries at one level of the structure, we search
     * progressively higher levels.  For example, when we run out of blk_
     * entries in a subdirectory, we search for the next subdirectory.
     * And so on.
     */
    @Override
    public ExtendedBlock nextBlock() throws IOException { //
      if (state.atEnd) {
        return null;
      }
      try {
        while (true) {
          List<String> entries = getSubdirEntries();
          if (entries != null) {
            state.curEntry = nextSorted(entries, state.curEntry);
            if (state.curEntry == null) {
              LOG.trace("nextBlock({}, {}): advancing from {} to next " +
                  "subdirectory.", storageID, bpid, state.curFinalizedSubDir);
            } else {
              ExtendedBlock block =
                  new ExtendedBlock(bpid, Block.filename2id(state.curEntry));

              /*
              File expectedBlockDir = DatanodeUtil.idToBlockDir(
                      new File("."), block.getBlockId());
              File actualBlockDir = Paths.get(".",
                  state.curFinalizedDir, state.curFinalizedSubDir).toFile();
              if (!expectedBlockDir.equals(actualBlockDir)) {
                LOG.error("nextBlock({}, {}): block id {} found in invalid " +
                    "directory.  Expected directory: {}.  " +
                    "Actual directory: {}", storageID, bpid,
                    block.getBlockId(), expectedBlockDir.getPath(),
                    actualBlockDir.getPath());
                continue;

              }
              */

              File blkFile = getBlockFile(bpid, block);
              File metaFile = FsDatasetUtil.findMetaFile(blkFile);
              block.setGenerationStamp(
                  Block.getGenerationStamp(metaFile.getName()));
              block.setNumBytes(blkFile.length());

              LOG.trace("nextBlock({}, {}): advancing to {}",
                  storageID, bpid, block);
              return block;
            }
          }
          state.curFinalizedSubDir = getNextFinalizedSubDir();
          if (state.curFinalizedSubDir == null) {
            state.curFinalizedDir = getNextFinalizedDir();
            if (state.curFinalizedDir == null) {
              state.atEnd = true;
              return null;
            }
          }
        }
      } catch (IOException e) {
        state.atEnd = true;
        LOG.error("nextBlock({}, {}): I/O error", storageID, bpid, e);
        throw e;
      }
    }

    private File getBlockFile(String bpid, ExtendedBlock blk)
        throws IOException {
     // return new File(DatanodeUtil.idToBlockDir(getFinalizedDir(bpid),
     //     blk.getBlockId()).toString() + "/" + blk.getBlockName());

      return new File(getFinalizedDir(bpid), blk.getBlockName());
    }

    @Override
    public boolean atEnd() {
      return state.atEnd;
    }

    @Override
    public void rewind() {
      cache = null;
      cacheMs = 0;
      state = new BlockIteratorState();
    }

    @Override
    public void save() throws IOException {
      state.lastSavedMs = Time.now();
      boolean success = false;
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(fileIoProvider.getFileOutputStream(
              FsVolumeImpl.this, getTempSaveFile()), "UTF-8"))) {
        WRITER.writeValue(writer, state);
        success = true;
      } finally {
        if (!success) {
          fileIoProvider.delete(FsVolumeImpl.this, getTempSaveFile());
        }
      }
      fileIoProvider.move(FsVolumeImpl.this,
          getTempSaveFile().toPath(), getSaveFile().toPath(),
          StandardCopyOption.ATOMIC_MOVE);
      if (LOG.isTraceEnabled()) {
        LOG.trace("save({}, {}): saved {}", storageID, bpid,
            WRITER.writeValueAsString(state));
      }
    }

    @Override
    public void load() throws IOException {
      File file = getSaveFile();
      this.state = READER.readValue(file);
      LOG.trace("load({}, {}): loaded iterator {} from {}: {}", storageID,
          bpid, name, file.getAbsoluteFile(),
          WRITER.writeValueAsString(state));
    }

    File getSaveFile() {
      return new File(bpidDir, name + ".cursor");
    }

    File getTempSaveFile() {
      return new File(bpidDir, name + ".cursor.tmp");
    }

    @Override
    public void setMaxStalenessMs(long maxStalenessMs) {
      this.maxStalenessMs = maxStalenessMs;
    }

    @Override
    public void close() throws IOException {
      // No action needed for this volume implementation.
    }

    @Override
    public long getIterStartMs() {
      return state.iterStartMs;
    }

    @Override
    public long getLastSavedMs() {
      return state.lastSavedMs;
    }

    @Override
    public String getBlockPoolId() {
      return bpid;
    }
  }

  @Override
  public BlockIterator newBlockIterator(String bpid, String name) {
    return new BlockIteratorImpl(bpid, name);
  }

  @Override
  public BlockIterator loadBlockIterator(String bpid, String name)
      throws IOException {
    BlockIteratorImpl iter = new BlockIteratorImpl(bpid, name);
    iter.load();
    return iter;
  }

  @Override
  public FsDatasetSpi<? extends FsVolumeSpi> getDataset() {
    return dataset;
  }


  @Override
  public LinkedList<ScanInfo> compileReport(String bpid,  LinkedList<ScanInfo> report, DirectoryScanner.ReportCompiler reportCompiler)
      throws InterruptedException, IOException {
    return compileReport(this, getFinalizedDir(bpid), report);
  }

  /**
   * RBW files. They get moved to the finalized block directory when
   * the block is finalized.
   */
  File createRbwFile(String bpid, Block b) throws IOException {
    // checkReference();
    reserveSpaceForReplica(b.getNumBytes());
    try {
    return getBlockPoolSlice(bpid).createRbwFile(b);
    } catch (IOException exception) {
      releaseReservedSpace(b.getNumBytes());
      throw exception;
    }
  }

  /**
   *
   * @param bytesReserved Space that was reserved during
   *     block creation. Now that the block is being finalized we
   *     can free up this space.
   * @return
   * @throws IOException
   */
  ReplicaInfo addFinalizedBlock(String bpid, Block b, ReplicaInfo replicaInfo,
                                long bytesReserved) throws IOException {
    releaseReservedSpace(bytesReserved);
    File dest = getBlockPoolSlice(bpid).addFinalizedBlock(b, replicaInfo);
    return new ReplicaBuilder(HdfsServerConstants.ReplicaState.FINALIZED)
            .setBlock(replicaInfo)
            .setFsVolume(this)
            .setDirectoryToUse(dest.getParentFile())
            .build();
  }

  void checkDirs() throws DiskErrorException {
    // TODO:FEDERATION valid synchronization
    for (BlockPoolSlice s : bpSlices.values()) {
      s.checkDirs();
    }
  }

  void getVolumeMap(ReplicaMap volumeMap) throws IOException {
    for (BlockPoolSlice s : bpSlices.values()) {
      s.getVolumeMap(volumeMap);
    }
  }

  void getVolumeMap(String bpid, ReplicaMap volumeMap) throws IOException {
    getBlockPoolSlice(bpid).getVolumeMap(volumeMap);
  }

  long getNumBlocks() {
    long numBlocks = 0;
    for (BlockPoolSlice s : bpSlices.values()) {
      numBlocks += s.getNumOfBlocks();
    }
    return numBlocks;
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
   * @throws IOException
   */
  void addToReplicasMap(String bpid, ReplicaMap volumeMap, File dir,
      boolean isFinalized) throws IOException {
    BlockPoolSlice bp = getBlockPoolSlice(bpid);
    // TODO move this up
    // dfsUsage.incDfsUsed(b.getNumBytes()+metaFile.length());
    bp.addToReplicasMap(volumeMap, dir, isFinalized);
  }

  void clearPath(String bpid, File f) throws IOException {
    getBlockPoolSlice(bpid).clearPath(f);
  }

  @Override
  public String toString() {
    return currentDir != null ? currentDir.getParent() : "NULL";
  }

  void shutdown() {
    Set<Map.Entry<String, BlockPoolSlice>> set = bpSlices.entrySet();
    for (Map.Entry<String, BlockPoolSlice> entry : set) {
      entry.getValue().shutdown();
    }
  }

  void addBlockPool(String bpid, Configuration conf) throws IOException {
    File bpdir = new File(currentDir, bpid);
    BlockPoolSlice bp = new BlockPoolSlice(bpid, this, bpdir, conf);
    bpSlices.put(bpid, bp);
  }
  
  void shutdownBlockPool(String bpid) {
    BlockPoolSlice bp = bpSlices.get(bpid);
    if (bp != null) {
      bp.shutdown();
    }
    bpSlices.remove(bpid);
  }

  boolean isBPDirEmpty(String bpid) throws IOException {
    File volumeCurrentDir = this.getCurrentDir();
    File bpDir = new File(volumeCurrentDir, bpid);
    File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    File finalizedDir =
        new File(bpCurrentDir, DataStorage.STORAGE_DIR_FINALIZED);
    File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
    if (fileIoProvider.exists(this, finalizedDir) &&
        !DatanodeUtil.dirNoFilesRecursive(this, finalizedDir, fileIoProvider)) {
      return false;
    }
    if (fileIoProvider.exists(this, rbwDir) &&
        fileIoProvider.list(this, rbwDir).length != 0) {
      return false;
    }
    return true;
  }
  
  void deleteBPDirectories(String bpid, boolean force) throws IOException {
    File volumeCurrentDir = this.getCurrentDir();
    File bpDir = new File(volumeCurrentDir, bpid);
    if (!bpDir.isDirectory()) {
      // nothing to be deleted
      return;
    }
    File tmpDir = new File(bpDir, DataStorage.STORAGE_DIR_TMP);
    File bpCurrentDir = new File(bpDir, DataStorage.STORAGE_DIR_CURRENT);
    File finalizedDir =
        new File(bpCurrentDir, DataStorage.STORAGE_DIR_FINALIZED);
    File rbwDir = new File(bpCurrentDir, DataStorage.STORAGE_DIR_RBW);
    if (force) {
      FileUtil.fullyDelete(bpDir);
    } else {
      if (!rbwDir.delete()) {
        throw new IOException("Failed to delete " + rbwDir);
      }
      if (!finalizedDir.delete()) {
        throw new IOException("Failed to delete " + finalizedDir);
      }
      FileUtil.fullyDelete(tmpDir);
      for (File f : FileUtil.listFiles(bpCurrentDir)) {
        if (!f.delete()) {
          throw new IOException("Failed to delete " + f);
        }
      }
      if (!bpCurrentDir.delete()) {
        throw new IOException("Failed to delete " + bpCurrentDir);
      }
      for (File f : FileUtil.listFiles(bpDir)) {
        if (!f.delete()) {
          throw new IOException("Failed to delete " + f);
        }
      }
      if (!bpDir.delete()) {
        throw new IOException("Failed to delete " + bpDir);
      }
    }
  }

  public ReplicaInPipeline createRbw(ExtendedBlock b) throws IOException {

    File f = createRbwFile(b.getBlockPoolId(), b.getLocalBlock());
    LocalReplicaInPipeline newReplicaInfo = new ReplicaBuilder(HdfsServerConstants.ReplicaState.RBW)
            .setBlockId(b.getBlockId())
            .setGenerationStamp(b.getGenerationStamp())
            .setFsVolume(this)
            .setDirectoryToUse(f.getParentFile())
            .setBytesToReserve(b.getNumBytes())
            .buildLocalReplicaInPipeline();
    return newReplicaInfo;
  }

  public ReplicaInPipeline convertTemporaryToRbw(ExtendedBlock b,
                                                 ReplicaInfo temp) throws IOException {

    final long blockId = b.getBlockId();
    final long expectedGs = b.getGenerationStamp();
    final long visible = b.getNumBytes();
    final long numBytes = temp.getNumBytes();

    // move block files to the rbw directory
    BlockPoolSlice bpslice = getBlockPoolSlice(b.getBlockPoolId());
    final File dest = FsDatasetImpl.moveBlockFiles(b.getLocalBlock(), temp,
            bpslice.getRbwDir());
    // create RBW
    final LocalReplicaInPipeline rbw = new ReplicaBuilder(HdfsServerConstants.ReplicaState.RBW)
            .setBlockId(blockId)
            .setLength(numBytes)
            .setGenerationStamp(expectedGs)
            .setFsVolume(this)
            .setDirectoryToUse(dest.getParentFile())
            .setWriterThread(Thread.currentThread())
            .setBytesToReserve(0)
            .buildLocalReplicaInPipeline();
    rbw.setBytesAcked(visible);

    // load last checksum and datalen
    final File destMeta = FsDatasetUtil.getMetaFile(dest,b.getGenerationStamp());
    byte[] lastChunkChecksum = loadLastPartialChunkChecksum(dest, destMeta);
    rbw.setLastChecksumAndDataLen(numBytes, lastChunkChecksum);
    return rbw;
  }

  /**
   * Compile list {@link ScanInfo} for the blocks in the directory <dir>
   */
  private LinkedList<ScanInfo> compileReport(FsVolumeSpi vol, File dir,
                                                              LinkedList<ScanInfo> report) {
    LOG.info("Scanning local blocks");
    File[] files;
    try {
      files = FileUtil.listFiles(dir);
    } catch (IOException ioe) {
      LOG.warn("Exception occured while compiling report: ", ioe);
      // Ignore this directory and proceed.
      return report;
    }
    Arrays.sort(files);


    List<File> blkFiles = new ArrayList();
    List<File> metaFiles = new ArrayList();
    List<File> subDirs = new ArrayList();
    for (File file : files) {
      if (!file.isDirectory()) {
        if (isBlockMetaFile("blk_", file.getName())) {
          metaFiles.add(file);
        } else if (Block.isBlockFilename(file)) {
          blkFiles.add(file);
        }
      } else {
        subDirs.add(file);
      }
    }

    for (File subDir : subDirs) {
      compileReport(vol, subDir, report);
    }

    for (int i = blkFiles.size() - 1; i >= 0; i--) {
      File blkFile = blkFiles.get(i);
      long blockId = Block.filename2id(blkFile.getName());
      File metaFile = popMetaFile(blkFile, metaFiles);
      report.add(new ScanInfo(blockId, blkFile, metaFile, vol));
      blkFiles.remove(i);
    }

    for (int i = metaFiles.size() - 1; i >= 0; i--) {
      File metaFile = metaFiles.get(i);
      long blockId = Block.getBlockId(metaFile.getName());
      report.add(new ScanInfo(blockId, null, metaFile, vol));
    }

    return report;
  }

  private static File popMetaFile(final File blkFile,
                                  final List<File> metaFiles) {
    for (File metaFile : metaFiles) {
      if (isBlockMetaFile(blkFile.getName() + "_", metaFile.getName())) {
        metaFiles.remove(metaFile);
        return metaFile;
      }
    }
    return null;
  }

  private static boolean isBlockMetaFile(String blockId, String metaFile) {
    return metaFile.startsWith(blockId) &&
            metaFile.endsWith(Block.METADATA_EXTENSION);
  }

  @Override
  public String getStorageID() {
    return storageID;
  }

  @Override
  public StorageType getStorageType() {
    return storageType;
  }

  DatanodeStorage toDatanodeStorage() {
    return new DatanodeStorage(storageID, DatanodeStorage.State.NORMAL, storageType);
  }

  @Override
  public byte[] loadLastPartialChunkChecksum(
          File blockFile, File metaFile) throws IOException {
    // readHeader closes the temporary FileInputStream.
    DataChecksum dcs;
    try (FileInputStream fis = fileIoProvider.getFileInputStream(
            this, metaFile)) {
      dcs = BlockMetadataHeader.readHeader(fis).getChecksum();
    }
    final int checksumSize = dcs.getChecksumSize();
    final long onDiskLen = blockFile.length();
    final int bytesPerChecksum = dcs.getBytesPerChecksum();

    if (onDiskLen % bytesPerChecksum == 0) {
      // the last chunk is a complete one. No need to preserve its checksum
      // because it will not be modified.
      return null;
    }

    long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
            (onDiskLen / bytesPerChecksum) * checksumSize;
    byte[] lastChecksum = new byte[checksumSize];
    try (RandomAccessFile raf = fileIoProvider.getRandomAccessFile(
            this, metaFile, "r")) {
      raf.seek(offsetInChecksum);
      int readBytes = raf.read(lastChecksum, 0, checksumSize);
      if (readBytes == -1) {
        throw new IOException("Expected to read " + checksumSize +
                " bytes from offset " + offsetInChecksum +
                " but reached end of file.");
      } else if (readBytes != checksumSize) {
        throw new IOException("Expected to read " + checksumSize +
                " bytes from offset " + offsetInChecksum + " but read " +
                readBytes + " bytes.");
      }
    }
    return lastChecksum;
  }

  public ReplicaInPipeline append(String bpid, ReplicaInfo replicaInfo,
      long newGS, long estimateBlockLen) throws IOException {

    long bytesReserved = estimateBlockLen - replicaInfo.getNumBytes();
    if (getAvailable() < bytesReserved) {
      throw new DiskChecker.DiskOutOfSpaceException("Insufficient space for appending to "
          + replicaInfo);
    }

    assert replicaInfo.getVolume() == this:
      "The volume of the replica should be the same as this volume";

    // construct a RBW replica with the new GS
    File newBlkFile = new File(getRbwDir(bpid), replicaInfo.getBlockName());
    LocalReplicaInPipeline newReplicaInfo = new ReplicaBuilder(HdfsServerConstants.ReplicaState.RBW)
        .setBlockId(replicaInfo.getBlockId())
        .setLength(replicaInfo.getNumBytes())
        .setGenerationStamp(newGS)
        .setFsVolume(this)
        .setDirectoryToUse(newBlkFile.getParentFile())
        .setWriterThread(Thread.currentThread())
        .setBytesToReserve(bytesReserved)
        .buildLocalReplicaInPipeline();

    // load last checksum and datalen
    LocalReplica localReplica = (LocalReplica)replicaInfo;
    byte[] lastChunkChecksum = loadLastPartialChunkChecksum(
            localReplica.getBlockFile(), localReplica.getMetaFile());
    newReplicaInfo.setLastChecksumAndDataLen(
            replicaInfo.getNumBytes(), lastChunkChecksum);

    // rename meta file to rbw directory
    // rename block file to rbw directory
    newReplicaInfo.moveReplicaFrom(replicaInfo, newBlkFile);

    reserveSpaceForReplica(bytesReserved);
    return newReplicaInfo;
  }

  @Override
  public FileIoProvider getFileIoProvider() {
    return fileIoProvider;
  }
}
