/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.security.token.Token;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, security token, etc).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LocatedBlock {

  /**
   * Comparator that ensures that a PROVIDED storage type is greater than any
   * other storage type. Any other storage types are considered equal.
   */
  private static class ProvidedLastComparator
          implements Comparator<DatanodeInfoWithStorage>, Serializable {

    private static final long serialVersionUID = 6441720011443190984L;

    @Override
    public int compare(DatanodeInfoWithStorage dns1,
                       DatanodeInfoWithStorage dns2) {
      if (StorageType.PROVIDED.equals(dns1.getStorageType())
              && !StorageType.PROVIDED.equals(dns2.getStorageType())) {
        return 1;
      }
      if (!StorageType.PROVIDED.equals(dns1.getStorageType())
              && StorageType.PROVIDED.equals(dns2.getStorageType())) {
        return -1;
      }
      // Storage types of dns1 and dns2 are now both provided or not provided;
      // thus, are essentially equal for the purpose of this comparator.
      return 0;
    }
  }

  private ExtendedBlock b;
  private long offset;  // offset of the first byte of the block in the file
  private DatanodeInfoWithStorage[] locs;
  // Storage type for each replica, if reported.
  private StorageType[] storageTypes;
  // Storage ID for each replica, if reported.
  private String[] storageIDs;
  // corrupt flag is true if all of the replicas of a block are corrupt.
  // else false. If block has few corrupt replicas, they are filtered and
  // their locations are not part of this object
  private boolean corrupt;
  private Token<BlockTokenIdentifier> blockToken =
      new Token<>();

  // use one instance of the Provided comparator as it uses no state.
  private static ProvidedLastComparator providedLastComparator =
          new ProvidedLastComparator();
  /**
   * List of cached datanode locations
   */
  private DatanodeInfo[] cachedLocs;

  private byte[] data = null;

  // Used when there are no locations
  private static final DatanodeInfoWithStorage[] EMPTY_LOCS = new DatanodeInfoWithStorage[0];


  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs) {
    this(b, convert(locs, null, null), null, null, -1, false, EMPTY_LOCS); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset, boolean corrupt) {
    this(b, convert(locs, null, null), null, null, startOffset, corrupt, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs, StorageType[] storageTypes) {
    this(b, convert(locs, storageIDs, storageTypes), storageIDs, storageTypes, -1, false, EMPTY_LOCS); // startOffset is unknown
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, String[] storageIDs,
                      StorageType[] storageTypes, long offset, boolean corrupt) {
    this(b, convert(locs, storageIDs, storageTypes), storageIDs, storageTypes, offset, corrupt, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages, long startOffset, boolean corrupt) {
    this(b, convert(DatanodeStorageInfo.toDatanodeInfos(storages), null, null),
        DatanodeStorageInfo.toStorageIDs(storages),
        DatanodeStorageInfo.toStorageTypes(storages),
        startOffset, corrupt, EMPTY_LOCS);
  }

  public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs,
        String[] storageIDs, StorageType[] storageTypes, long startOffset,
        boolean corrupt, DatanodeInfo[] cachedLocs) {

    this(b, convert(locs, storageIDs, storageTypes), storageIDs, storageTypes, startOffset, corrupt,
            null == cachedLocs || 0 == cachedLocs.length ? EMPTY_LOCS : cachedLocs);
  }

  /**
   * Main constructor for {@link LocatedBlocks}
   */
  public LocatedBlock(ExtendedBlock b, DatanodeInfoWithStorage[] locs,
                      String[] storageIDs, StorageType[] storageTypes, long startOffset,
                      boolean corrupt, DatanodeInfo[] cachedLocs) {
    this.b = b;
    this.offset = startOffset;
    this.corrupt = corrupt;
    if(corrupt) {
      System.err.println("corrupt located block ! ");
    }
    this.locs = null == locs ? EMPTY_LOCS : locs;
    System.out.println("locs.length = "+ locs.length); // TODO: GABRIEL - locations.length does not match most of the time
    this.storageIDs = storageIDs;
    this.storageTypes = storageTypes;
    this.cachedLocs = null == cachedLocs || 0 == cachedLocs.length
            ? EMPTY_LOCS
            : cachedLocs;
  }

  public static LocatedBlock createLocatedBlock(ExtendedBlock b,
                                                DatanodeStorageInfo[] storages, long startOffset, boolean corrupt) {

    final DatanodeInfo[] locs = new DatanodeInfo[storages.length];
    final String[] storageIDs = new String[storages.length];
    final StorageType[] storageType = new StorageType[storages.length];
    for(int i = 0; i < storages.length; i++) {
      locs[i] = storages[i].getDatanodeDescriptor();
      storageIDs[i] = storages[i].getStorageID();
      storageType[i] = storages[i].getStorageType();
    }
    return new LocatedBlock(b, convert(locs, storageIDs, storageType), storageIDs, storageType, startOffset, corrupt, EMPTY_LOCS);
  }


  public void setData(byte[] data) {
      if(isPhantomBlock()){
        this.data = data;
      } else {
        throw new UnsupportedOperationException("Can not set data. Not a phantom data block");
      }
  }

  public boolean isPhantomBlock(){
    if (b.getBlockId() < 0) {
        return true;
    }
    return  false;
  }

  public boolean isDataSet(){
     if(isPhantomBlock() && data!= null && data.length > 0) {
       return true;
     }else{
       return false;
     }
  }

  public byte[] getData() {
    String error = null;
    if(isDataSet()) {
      return data;
    }else if(isPhantomBlock()) {
      error = "The file data is not set";
    }else {
      error = "The file data is not stored in the database";
    }
    throw new UnsupportedOperationException(error);
  }

  private static DatanodeInfoWithStorage[] convert(
          DatanodeInfo[] infos, String[] storageIDs, StorageType[] storageTypes) {
    if (null == infos) {
      return EMPTY_LOCS;
    }

    DatanodeInfoWithStorage[] ret = new DatanodeInfoWithStorage[infos.length];
    for(int i = 0; i < infos.length; i++) {
      ret[i] = new DatanodeInfoWithStorage(infos[i],
              storageIDs   != null ? storageIDs[i]   : null,
              storageTypes != null ? storageTypes[i] : null);
    }
    return ret;
  }

  /**
   * @return Datanodes with a cached block replica
   */
  public DatanodeInfo[] getCachedLocations() {
    return cachedLocs;
  }

  public Token<BlockTokenIdentifier> getBlockToken() {
    return blockToken;
  }

  public StorageType[] getStorageTypes() {
    return storageTypes;
  }

  public String[] getStorageIDs() {
    return storageIDs;
  }

  public void setBlockToken(Token<BlockTokenIdentifier> token) {
    this.blockToken = token;
  }

  public ExtendedBlock getBlock() {
    return b;
  }

  public DatanodeInfo[] getLocations() {
    return locs;
  }

  public DatanodeInfo[] getUniqueLocations() {
    HashSet<DatanodeInfo> dns = new HashSet<DatanodeInfo>();
    for(DatanodeInfo dn : this.locs) {
      dns.add(dn);
    }
    return dns.toArray(new DatanodeInfo[0]);
  }

  public long getStartOffset() {
    return offset;
  }

  public long getBlockSize() {
    return b.getNumBytes();
  }

  void setStartOffset(long value) {
    this.offset = value;
  }

  void setCorrupt(boolean corrupt) {
    this.corrupt = corrupt;
  }

  public boolean isCorrupt() {
    return this.corrupt;
  }


  /**
   * Add a the location of a cached replica of the block.
   *
   * @param loc of datanode with the cached replica
   */
  public void addCachedLoc(DatanodeInfo loc) {
    List<DatanodeInfo> cachedList = Lists.newArrayList(cachedLocs);
    if (cachedList.contains(loc)) {
      return;
    }
    // Try to re-use a DatanodeInfo already in loc
    for (DatanodeInfoWithStorage di : locs) {
      if (loc.equals(di)) {
        cachedList.add(di);
        cachedLocs = cachedList.toArray(cachedLocs);
        return;
      }
    }
    // Not present in loc, add it and go
    cachedList.add(loc);
    Preconditions.checkArgument(cachedLocs != EMPTY_LOCS,
            "Cached locations should only be added when having a backing"
                    + " disk replica!", loc, locs.length, Arrays.toString(locs));
    cachedLocs = cachedList.toArray(cachedLocs);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + b + "; getBlockSize()=" +
        getBlockSize() + "; corrupt=" + corrupt + "; offset=" + offset +
        "; locs=" + java.util.Arrays.asList(locs) + "}";
  }

  public final static Comparator<LocatedBlock> blockIdComparator =
      new Comparator<LocatedBlock>() {

        @Override
        public int compare(LocatedBlock locatedBlock,
            LocatedBlock locatedBlock2) {
          if (locatedBlock.getBlock().getBlockId() <
              locatedBlock2.getBlock().getBlockId()) {
            return -1;
          }
          if (locatedBlock.getBlock().getBlockId() >
              locatedBlock2.getBlock().getBlockId()) {
            return 1;
          }
          return 0;
        }
      };

  /**
   * Moves all locations that have {@link org.apache.hadoop.fs.StorageType}
   * {@code PROVIDED} to the end of the locations array without
   * changing the relative ordering of the remaining locations
   * Only the first {@code activeLen} locations are considered.
   * The caller must immediately invoke {updateCachedStorageInfo}
   * to update the cached Storage ID/Type arrays.
   * @param activeLen
   */
  public void moveProvidedToEnd(int activeLen) {

    if (activeLen <= 0) {
      return;
    }
    // as this is a stable sort, for elements that are equal,
    // the current order of the elements is maintained
    Arrays.sort(locs, 0, (activeLen < locs.length) ? activeLen : locs.length,
            providedLastComparator);
  }
}
