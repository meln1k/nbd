/*
 * Copyright 2018 Sam Pullara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nbdfdb;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Ints;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Logger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Block storage in FDB
 */
public class FDBArray {


  private final Logger log;


  // FDB
  private static final byte[] ONE = new byte[]{0, 0, 0, 0, 0, 0, 0, 1};
  private static final byte[] MINUS_ONE = new byte[]{0, 0, 0, 0, 0, 0, 0, -1};
  private static DirectoryLayer dl = DirectoryLayer.getDefault();

  // Metadata keys
  private static final String BLOCK_SIZE_KEY = "block_size";
  private static final String BLOCKS = "blocks";

  // Location in the database
  private final DirectorySubspace metadata;
  private final DirectorySubspace data;
  private final Database database;
  private final int blockSize;
  private final DirectorySubspace ds;
  private final FDBBitSet usedBlocks;

  // Other constants
  private final int WRITE_GROUP_SIZE = 4;

  private static final ExecutorService es = Executors.newFixedThreadPool(64, r -> new Thread(r, "fdbarray"));


  // Used for copies
  private final ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[blockSize];
    }
  };

  public static FDBArray open(Database database, String name) {
    DirectorySubspace ds = get(dl.open(database, asList("com.sampullara.fdb.array", name)));
    return new FDBArray(database, ds);
  }


  public static FDBArray create(Database database, String name, int blockSize) {
    DirectorySubspace ds = get(dl.create(database, asList("com.sampullara.fdb.array", name)));
    return create(database, ds, blockSize);
  }

  protected static FDBArray create(Database database, DirectorySubspace ds, int blockSize) {
    DirectorySubspace metadata = get(ds.create(database, singletonList("metadata")));

    database.run((Function<Transaction, Void>) tx -> {
      tx.set(metadata.get(BLOCK_SIZE_KEY).pack(), Ints.toByteArray(blockSize));
      return null;
    });
    return new FDBArray(database, ds);
  }

  private static <T> T get(CompletableFuture<T> future) {
    try {
      return future.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected FDBArray(Database database, DirectorySubspace ds) {
    log = Logger.getLogger("FDBArray");
    this.ds = ds;
    this.database = database;
    this.metadata = get(ds.createOrOpen(database, singletonList("metadata")));
    this.data = get(ds.createOrOpen(database, singletonList("data")));
    Integer currentBlocksize = database.run(tx -> {
      byte[] key = metadata.get(BLOCK_SIZE_KEY).pack();
      byte[] currentBlockSize = get(tx.get(key));
      if (currentBlockSize == null) {
        return null;
      } else {
        return Ints.fromByteArray(currentBlockSize);
      }
    });
    if (currentBlocksize == null) {
      throw new IllegalArgumentException("Block size for array not configured");
    } else {
      blockSize = currentBlocksize;
    }
    usedBlocks = new FDBBitSet(database, metadata.get(BLOCKS), 512);
  }

  private Boolean notAligned(int blockOffset, int length, int lastBlockLength) {
    return blockOffset > 0 || (blockOffset == 0 && length < blockSize) || (lastBlockLength != 0 && lastBlockLength != blockSize);
  }


  public CompletableFuture<Void> write(byte[] write, long offset) {
    // Calculate the block locations
    int length = write.length;
    long firstBlock = offset / blockSize;
    long lastBlock = (offset + length) / blockSize;
    int blockOffset = (int) (offset % blockSize);
    int shift = blockSize - blockOffset;

    int lastBlockPosition = (int) ((lastBlock - firstBlock - 1) * blockSize + shift);
    int lastBlockLength = length - lastBlockPosition;

    // Track where we have written so we can estimate usage later
    usedBlocks.set(firstBlock, lastBlock);

    if (notAligned(blockOffset, length, lastBlockLength)) {
      log.warning("Non-aligned write!");
      CompletableFuture<Void> firstBlockFuture = database.runAsync(tx -> {
        // Use a single buffer for all full blocksize writes
        byte[] bytes = buffer.get();


        // Special case first block and last block
        byte[] firstBlockKey = data.get(firstBlock).pack();
        if (blockOffset > 0 || (blockOffset == 0 && length < blockSize)) {
          // Only need to do this if the first block is partial
          byte[] readBytes = new byte[blockSize];
          read(tx, firstBlock * blockSize, readBytes, null);
          int writeLength = Math.min(length, shift);
          System.arraycopy(write, 0, readBytes, blockOffset, writeLength);
          tx.set(firstBlockKey, readBytes);
        } else {
          // In this case copy the full first block blindly
          System.arraycopy(write, 0, bytes, 0, blockSize);
          tx.set(firstBlockKey, bytes);
        }
        return CompletableFuture.completedFuture(null);
      }, es);

      // If there is more than one block
      if (lastBlock > firstBlock) {
        // For the blocks in the middle we can just blast values in without looking at the current bytes

        CompletableFuture<Void> middleWrites = writeAligned(write, firstBlock, firstBlock + 1, lastBlock, shift);

        CompletableFuture<Void> lastBlockFuture = database.runAsync(tx -> {

          byte[] bytes = buffer.get();

          byte[] lastBlockKey = data.get(lastBlock).pack();
          // If the last block is a complete block we don't need to read
          if (lastBlockLength == blockSize) {
            System.arraycopy(write, lastBlockPosition, bytes, 0, blockSize);
            tx.set(lastBlockKey, bytes);
          } else {
            byte[] readBytes = new byte[blockSize];
            read(tx, lastBlock * blockSize, readBytes, null);
            System.arraycopy(write, lastBlockPosition, readBytes, 0, lastBlockLength);
            tx.set(lastBlockKey, readBytes);
          }
          return CompletableFuture.completedFuture(null);

        }, es);

        return CompletableFuture.allOf(firstBlockFuture, middleWrites, lastBlockFuture);

      } else {
        return firstBlockFuture;
      }


      } else {
        return writeAligned(write, firstBlock, firstBlock, lastBlock, shift);

      }

  }

  /**
   * Performs an aligned write in group of 4 blocks. Blocks MUST be aligned!
   *
   * @param write array to copy data from
   * @param startBlock start block
   * @param endBlock end block (exclusive)
   * @return
   */
  private CompletableFuture<Void> writeAligned(byte[] write, long firstBlock, long startBlock, long endBlock, int shift) {

    long totalBlocksToWrite = (endBlock - startBlock);
    int completeGroups = (int)(totalBlocksToWrite / WRITE_GROUP_SIZE);
    long lastGroupSize = totalBlocksToWrite % WRITE_GROUP_SIZE;
    boolean incompleteLastGroup = lastGroupSize != 0;

    int futuresSize = completeGroups;
    if (incompleteLastGroup) {
      futuresSize++;
    }

    List<CompletableFuture<Void>> futures = new ArrayList<>(futuresSize);


    // start all writes in a group size


    for (long groupPosition = startBlock; groupPosition < startBlock + completeGroups * WRITE_GROUP_SIZE; groupPosition += WRITE_GROUP_SIZE) {
      final long currentGroupPosition = groupPosition;
      CompletableFuture<Void> writeResult = database.runAsync(tx -> {
        byte[] bytes = buffer.get();
        for (long i = currentGroupPosition; i < currentGroupPosition + WRITE_GROUP_SIZE; i++) {
          byte[] key = data.pack(i);
          int writeBlock = (int) (i - firstBlock);
          int position = (writeBlock - 1) * blockSize + shift;
          System.arraycopy(write, position, bytes, 0, blockSize);
          tx.set(key, bytes);
        }
        return CompletableFuture.completedFuture(null);
      }, es);

      futures.add(writeResult);
    }



    if (incompleteLastGroup) {
      CompletableFuture<Void> writeResult = database.runAsync(tx -> {
        byte[] bytes = buffer.get();
        for (long i = startBlock + completeGroups * WRITE_GROUP_SIZE; i < endBlock; i++) {
          byte[] key = data.pack(i);
          int writeBlock = (int) (i - firstBlock);
          int position = (writeBlock - 1) * blockSize + shift;
          System.arraycopy(write, position, bytes, 0, blockSize);
          tx.set(key, bytes);
        }
        return CompletableFuture.completedFuture(null);
      }, es);

      futures.add(writeResult);
    }



    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futuresSize]));
  }

  public CompletableFuture<Long> usage() {
    return usedBlocks.count().thenApply(usedBlocks -> usedBlocks * blockSize);
  }

  /**
   * Read latest blocks.
   *
   * @param read
   * @param offset
   * @return
   */
  public CompletableFuture<Void> read(byte[] read, long offset) {
    return database.runAsync(tx -> {
      read(tx, offset, read, null);
      return CompletableFuture.completedFuture(null);
    }, es);
  }

  static class BlocksRead {
    private final int total;
    private Set<Long> blocksRead;

    BlocksRead(int total) {
      this.total = total;
      blocksRead = new HashSet<>(total);
    }

    boolean done() {
      return blocksRead.size() == total;
    }

    boolean read(long block) {
      return blocksRead.add(block);
    }
  }

  private void read(ReadTransaction tx, long offset, byte[] read, BlocksRead blocksRead) {
    long firstBlock = offset / blockSize;
    int blockOffset = (int) (offset % blockSize);
    int length = read.length;
    long lastBlock = (offset + length) / blockSize;
    long currentBlockId = -1;
    byte[] currentValue = null;
    if (blocksRead == null) {
      blocksRead = new BlocksRead((int) (lastBlock - firstBlock + 1));
    }
    List<KeyValue> range =  get(tx.getRange(data.get(firstBlock).pack(), data.get(lastBlock + 1).pack()).asList());

    for (KeyValue keyValue : range) {
      Tuple keyTuple = data.unpack(keyValue.getKey());
      long blockId = keyTuple.getLong(0);
      if (blockId != currentBlockId && currentBlockId != -1) {
        // Only copy blocks that we are going to use
        copy(read, firstBlock, blockOffset, lastBlock, currentValue, currentBlockId, blocksRead);
        currentValue = null;
      }
      // Advance the current block id
      currentBlockId = blockId;
      // Update the current value with the latest value not written after the snapshot timestamp
      currentValue = keyValue.getValue();
    }
    copy(read, firstBlock, blockOffset, lastBlock, currentValue, currentBlockId, blocksRead);

  }

  private void copy(byte[] read, long firstBlock, int blockOffset, long lastBlock, byte[] currentValue, long blockId, BlocksRead blocksRead) {
    if (currentValue != null) {
      if (blocksRead == null || blocksRead.read(blockId)) {
        int blockPosition = (int) ((blockId - firstBlock) * blockSize);
        int shift = blockSize - blockOffset;
        if (blockId == firstBlock) {
          int firstBlockLength = Math.min(shift, read.length);
          System.arraycopy(currentValue, blockOffset, read, 0, firstBlockLength);
        } else {
          int position = blockPosition - blockSize + shift;
          if (blockId == lastBlock) {
            int lastLength = read.length - position;
            System.arraycopy(currentValue, 0, read, position, lastLength);
          } else {
            System.arraycopy(currentValue, 0, read, position, blockSize);
          }
        }
      }
    }
  }


  public void clear() {
    database.run((Function<Transaction, Void>) tx -> {
      tx.clear(data.pack());
      usedBlocks.clear(tx);
      return null;
    });
  }

  public void delete() {
    get(ds.remove(database));
  }

  public void setMetadata(byte[] key, byte[] value) {
    database.run(tx -> {
      tx.set(metadata.get(key).pack(), value);
      return null;
    });
  }

  public byte[] getMetadata(byte[] key) {
    byte[] value = database.run(tx -> get(tx.get(metadata.get(key).pack())));
    return value;
  }
}
