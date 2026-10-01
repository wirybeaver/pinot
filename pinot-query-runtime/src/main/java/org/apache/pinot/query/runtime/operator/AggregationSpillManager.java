/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Manages hash-partitioned aggregation spill files for one operator. The caller must finish reading before calling
/// [#close()], which recursively removes the operator-scoped directory. This class is not thread-safe.
@SuppressWarnings("rawtypes")
public class AggregationSpillManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationSpillManager.class);
  private static final String SPILL_FILE_PREFIX = "partition-";
  private static final String SPILL_FILE_SUFFIX = ".spill";
  private static final String SPILL_SCOPE = "AggregationSpillManager#spill";
  private static final String RESTORE_SCOPE = "AggregationSpillManager#consumePartition";
  private static final int MAX_ROWS_PER_RECORD = 1024;
  private static final int MAX_BUFFERED_ROWS = 8192;
  private static final Map<String, Long> QUERY_SPILL_BYTES = new HashMap<>();
  private static long _processSpillBytes;

  private final int _numPartitions;
  private final int _numGroupKeys;
  private final DataSchema _spillSchema;
  private final AggregationFunction[] _aggFunctions;
  private final Path _spillDirectory;
  private final long _maxSpillBytes;
  private final long _maxServerSpillBytes;
  private final String _queryId;
  private final long[] _partitionBytes;
  private long _reservedBytes;
  private final FileChannel[] _spillWriters;
  private final ByteBuffer _recordLengthBuffer = ByteBuffer.allocate(Integer.BYTES);

  AggregationSpillManager(int numPartitions, int numGroupKeys, DataSchema spillSchema,
      AggregationFunction[] aggFunctions, Path spillRoot, long maxSpillBytes, long maxServerSpillBytes,
      String queryId) {
    if (numPartitions <= 0 || numPartitions > Server.MAX_MSE_AGGREGATION_SPILL_PARTITIONS) {
      throw new IllegalArgumentException(
          "Number of spill partitions must be between 1 and " + Server.MAX_MSE_AGGREGATION_SPILL_PARTITIONS);
    }
    _numPartitions = numPartitions;
    _numGroupKeys = numGroupKeys;
    _spillSchema = spillSchema;
    _aggFunctions = aggFunctions;
    _maxSpillBytes = maxSpillBytes;
    _maxServerSpillBytes = maxServerSpillBytes;
    _queryId = queryId;
    _partitionBytes = new long[numPartitions];
    _spillWriters = new FileChannel[numPartitions];
    try {
      Files.createDirectories(spillRoot);
      _spillDirectory = Files.createTempDirectory(spillRoot, "pinot-aggregation-spill-");
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create aggregation spill directory", e);
    }
  }

  /// Remove only directories owned by aggregation spill. Call at startup before any query uses this instance's
  /// dedicated spill root. The root must not be shared by concurrently running server instances.
  public static void cleanOrphanedSpillFiles(Path spillRoot) {
    if (!Files.exists(spillRoot)) {
      return;
    }
    try (var children = Files.list(spillRoot)) {
      for (Path child : children.filter(path -> path.getFileName().toString().startsWith("pinot-aggregation-spill-"))
          .toList()) {
        deleteSpillDirectory(child);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to clean aggregation spill root: " + spillRoot, e);
    }
  }

  SpillResult spill(Iterator<Object[]> rows) {
    List<Object[]>[] partitions = createPartitions();
    int numRows = 0;
    int numBufferedRows = 0;
    long serializedBytes = 0;
    while (rows.hasNext()) {
      QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numRows, SPILL_SCOPE);
      Object[] row = rows.next();
      int partitionId = getPartition(row);
      List<Object[]> partition = partitions[partitionId];
      if (partition == null) {
        partition = new ArrayList<>();
        partitions[partitionId] = partition;
      }
      partition.add(row);
      numRows++;
      numBufferedRows++;
      if (partition.size() == MAX_ROWS_PER_RECORD) {
        serializedBytes += appendPartition(partitionId, partition);
        partition.clear();
        numBufferedRows -= MAX_ROWS_PER_RECORD;
      }
      if (numBufferedRows == MAX_BUFFERED_ROWS) {
        serializedBytes += flushPartitions(partitions);
        numBufferedRows = 0;
      }
    }
    serializedBytes += flushPartitions(partitions);
    return new SpillResult(numRows, serializedBytes);
  }

  boolean hasPartition(int partitionId) {
    return _partitionBytes[partitionId] != 0;
  }

  /// Consumes all records from a partition and deletes its file after the attempt, including when reading or
  /// processing fails.
  void consumePartition(int partitionId, Consumer<MseBlock.Data> consumer) {
    if (!hasPartition(partitionId)) {
      return;
    }
    Path spillFile = getSpillFile(partitionId);

    try (DataInputStream input =
        new DataInputStream(new BufferedInputStream(Files.newInputStream(spillFile)))) {
      closeSpillWriter(partitionId);
      long remainingBytes = Files.size(spillFile);
      int numRecordsRead = 0;
      while (remainingBytes > 0) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(numRecordsRead++, RESTORE_SCOPE);
        int recordLength = input.readInt();
        remainingBytes -= Integer.BYTES;
        if (recordLength <= 0 || recordLength > remainingBytes) {
          throw new IOException("Invalid spill record length " + recordLength + " in: " + spillFile);
        }
        byte[] bytes = new byte[recordLength];
        input.readFully(bytes);
        remainingBytes -= recordLength;
        DataBlock dataBlock = DataBlockUtils.readFrom(ByteBuffer.wrap(bytes));
        consumer.accept(new SerializedDataBlock(dataBlock));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read aggregation spill partition: " + partitionId, e);
    } finally {
      try {
        Files.deleteIfExists(spillFile);
        releaseSpillBytes(_partitionBytes[partitionId]);
        _partitionBytes[partitionId] = 0;
      } catch (IOException e) {
        LOGGER.warn("Failed to delete consumed aggregation spill partition; close will retry: {}", spillFile, e);
      }
    }
  }

  Path getSpillDirectory() {
    return _spillDirectory;
  }

  @Override
  public void close() {
    RuntimeException failure = null;
    try {
      closeSpillWriters();
    } catch (RuntimeException e) {
      failure = e;
    }
    try {
      if (Files.exists(_spillDirectory)) {
        deleteSpillDirectory(_spillDirectory);
      }
    } catch (RuntimeException e) {
      if (failure != null) {
        failure.addSuppressed(e);
      } else {
        failure = e;
      }
    } finally {
      if (!Files.exists(_spillDirectory)) {
        releaseSpillBytes(_reservedBytes);
        Arrays.fill(_partitionBytes, 0);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void deleteSpillDirectory(Path directory) {
    try {
      Files.walkFileTree(directory, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path directory, IOException exception)
            throws IOException {
          if (exception != null) {
            throw exception;
          }
          Files.delete(directory);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to delete aggregation spill directory: " + directory, e);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object[]>[] createPartitions() {
    return new List[_numPartitions];
  }

  private long flushPartitions(List<Object[]>[] partitions) {
    long serializedBytes = 0;
    for (int partitionId = 0; partitionId < _numPartitions; partitionId++) {
      List<Object[]> partition = partitions[partitionId];
      if (partition != null && !partition.isEmpty()) {
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(partitionId, SPILL_SCOPE);
        serializedBytes += appendPartition(partitionId, partition);
        partition.clear();
      }
    }
    return serializedBytes;
  }

  /// Equal keys after DataBlock serialization must hash to the same partition here; otherwise restore can emit
  /// duplicate groups. In particular, hash array keys by content, not by identity.
  private int getPartition(Object[] row) {
    int hash = 1;
    for (int i = 0; i < _numGroupKeys; i++) {
      hash = 31 * hash + deepHashCode(row[i]);
    }
    return Math.floorMod(hash, _numPartitions);
  }

  private static int deepHashCode(Object value) {
    if (value instanceof Object[]) {
      return Arrays.deepHashCode((Object[]) value);
    }
    if (value instanceof byte[]) {
      return Arrays.hashCode((byte[]) value);
    }
    if (value instanceof short[]) {
      return Arrays.hashCode((short[]) value);
    }
    if (value instanceof int[]) {
      return Arrays.hashCode((int[]) value);
    }
    if (value instanceof long[]) {
      return Arrays.hashCode((long[]) value);
    }
    if (value instanceof char[]) {
      return Arrays.hashCode((char[]) value);
    }
    if (value instanceof float[]) {
      return Arrays.hashCode((float[]) value);
    }
    if (value instanceof double[]) {
      return Arrays.hashCode((double[]) value);
    }
    if (value instanceof boolean[]) {
      return Arrays.hashCode((boolean[]) value);
    }
    return value != null ? value.hashCode() : 0;
  }

  private long appendPartition(int partitionId, List<Object[]> rows) {
    DataBlock dataBlock = new RowHeapDataBlock(rows, _spillSchema, _aggFunctions).asSerialized().getDataBlock();
    try {
      List<ByteBuffer> buffers = dataBlock.serialize();
      int recordLength = getRecordLength(buffers);
      reserveSpillBytes(partitionId, Integer.BYTES + (long) recordLength);
      FileChannel output = getSpillWriter(partitionId);
      _recordLengthBuffer.clear();
      _recordLengthBuffer.putInt(recordLength).flip();
      writeFully(output, _recordLengthBuffer);
      for (ByteBuffer buffer : buffers) {
        writeFully(output, buffer);
      }
      return Integer.BYTES + (long) recordLength;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to spill aggregation partition: " + partitionId, e);
    }
  }

  private void reserveSpillBytes(int partitionId, long bytes) {
    synchronized (QUERY_SPILL_BYTES) {
      long queryBytes = QUERY_SPILL_BYTES.getOrDefault(_queryId, 0L);
      if (bytes > _maxSpillBytes - queryBytes) {
        throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException("Query aggregation spill byte limit exceeded");
      }
      if (bytes > _maxServerSpillBytes - _processSpillBytes) {
        throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException("Server aggregation spill byte limit exceeded");
      }
      QUERY_SPILL_BYTES.put(_queryId, queryBytes + bytes);
      _processSpillBytes += bytes;
      _reservedBytes += bytes;
      _partitionBytes[partitionId] += bytes;
    }
  }

  private void releaseSpillBytes(long bytes) {
    synchronized (QUERY_SPILL_BYTES) {
      _processSpillBytes -= bytes;
      long remaining = QUERY_SPILL_BYTES.getOrDefault(_queryId, 0L) - bytes;
      if (remaining == 0) {
        QUERY_SPILL_BYTES.remove(_queryId);
      } else {
        QUERY_SPILL_BYTES.put(_queryId, remaining);
      }
      _reservedBytes -= bytes;
    }
  }

  private static int getRecordLength(List<ByteBuffer> buffers) {
    long length = 0;
    for (ByteBuffer buffer : buffers) {
      length += buffer.remaining();
    }
    return Math.toIntExact(length);
  }

  private FileChannel getSpillWriter(int partitionId)
      throws IOException {
    FileChannel writer = _spillWriters[partitionId];
    if (writer != null) {
      return writer;
    }
    writer = FileChannel.open(getSpillFile(partitionId), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.APPEND);
    _spillWriters[partitionId] = writer;
    return writer;
  }

  private static void writeFully(FileChannel output, ByteBuffer buffer)
      throws IOException {
    while (buffer.hasRemaining()) {
      output.write(buffer);
    }
  }

  private void closeSpillWriter(int partitionId)
      throws IOException {
    FileChannel writer = _spillWriters[partitionId];
    if (writer != null) {
      writer.close();
      _spillWriters[partitionId] = null;
    }
  }

  private void closeSpillWriters() {
    IOException failure = null;
    for (int partitionId = 0; partitionId < _numPartitions; partitionId++) {
      try {
        closeSpillWriter(partitionId);
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw new UncheckedIOException("Failed to close aggregation spill files", failure);
    }
  }

  private Path getSpillFile(int partitionId) {
    return _spillDirectory.resolve(SPILL_FILE_PREFIX + partitionId + SPILL_FILE_SUFFIX);
  }

  record SpillResult(int rows, long bytes) {
  }
}
