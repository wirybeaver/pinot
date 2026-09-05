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
package org.apache.pinot.query.service.dispatch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


/// Computes a deterministic contiguous assignment for coalescing materialized shuffle partitions.
///
/// This class only defines the adaptive policy. Applying the returned assignment is safe only after complete producer
/// statistics are available and before the consumer stage is dispatched. Invalid byte measurements are rejected with
/// `IllegalArgumentException`. The class is stateless and thread-safe.
final class AdaptivePartitionCoalescer {
  private static final double SMALL_PARTITION_FACTOR = 0.2;
  private static final double MERGED_PARTITION_FACTOR = 1.2;

  private AdaptivePartitionCoalescer() {
  }

  /// Groups ordered source partitions toward `targetBytes`.
  ///
  /// @return immutable, contiguous half-open partition ranges
  static List<PartitionGroup> coalesce(long[] partitionBytes, long targetBytes) {
    if (targetBytes <= 0) {
      throw new IllegalArgumentException("Target bytes must be positive");
    }
    for (long partitionSize : partitionBytes) {
      if (partitionSize < 0) {
        throw new IllegalArgumentException("Partition bytes must be non-negative");
      }
    }
    int numPartitions = partitionBytes.length;
    if (numPartitions == 0) {
      return List.of();
    }

    List<Integer> groupStarts = new ArrayList<>();
    groupStarts.add(0);
    long currentGroupBytes = 0;
    boolean currentGroupContainsOversizedPartition = false;
    GroupStats previousGroup = null;
    for (int i = 0; i < numPartitions; i++) {
      long partitionSize = partitionBytes[i];
      if (i > 0 && partitionSize > targetBytes - currentGroupBytes) {
        previousGroup = closeCurrentGroup(groupStarts,
            new GroupStats(BigInteger.valueOf(currentGroupBytes), currentGroupContainsOversizedPartition),
            previousGroup, targetBytes);
        groupStarts.add(i);
        currentGroupBytes = partitionSize;
        currentGroupContainsOversizedPartition = partitionSize > targetBytes;
      } else {
        currentGroupBytes += partitionSize;
        currentGroupContainsOversizedPartition |= partitionSize > targetBytes;
      }
    }
    closeCurrentGroup(groupStarts,
        new GroupStats(BigInteger.valueOf(currentGroupBytes), currentGroupContainsOversizedPartition), previousGroup,
        targetBytes);

    if (groupStarts.size() == numPartitions) {
      return originalAssignment(numPartitions);
    }
    List<PartitionGroup> groups = new ArrayList<>(groupStarts.size());
    for (int i = 0; i < groupStarts.size(); i++) {
      int end = i + 1 < groupStarts.size() ? groupStarts.get(i + 1) : numPartitions;
      groups.add(new PartitionGroup(groupStarts.get(i), end));
    }
    return List.copyOf(groups);
  }

  private static GroupStats closeCurrentGroup(List<Integer> groupStarts, GroupStats currentGroup,
      GroupStats previousGroup, long targetBytes) {
    if (previousGroup == null || previousGroup.containsOversizedPartition()
        || currentGroup.containsOversizedPartition()) {
      return currentGroup;
    }
    BigInteger mergedBytes = previousGroup.bytes().add(currentGroup.bytes());
    boolean shouldMerge = mergedBytes.doubleValue() < targetBytes * MERGED_PARTITION_FACTOR
        || currentGroup.bytes().doubleValue() < targetBytes * SMALL_PARTITION_FACTOR
        || previousGroup.bytes().doubleValue() < targetBytes * SMALL_PARTITION_FACTOR;
    if (shouldMerge) {
      groupStarts.remove(groupStarts.size() - 1);
      return new GroupStats(mergedBytes, false);
    }
    return currentGroup;
  }

  private static List<PartitionGroup> originalAssignment(int numPartitions) {
    List<PartitionGroup> groups = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      groups.add(new PartitionGroup(i, i + 1));
    }
    return List.copyOf(groups);
  }

  /// An immutable contiguous half-open source-partition range. Instances are thread-safe.
  record PartitionGroup(int startInclusive, int endExclusive) {
  }

  /// Immutable statistics for one candidate group. Instances are thread-safe.
  private record GroupStats(BigInteger bytes, boolean containsOversizedPartition) {
  }
}
