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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.WorkerMetadata;


/// Validates materialized output coverage and deterministically assigns contiguous logical partitions to workers.
/// A positive target groups adjacent partitions by total file bytes across producers; zero keeps one partition per
/// worker. Original partition identities are preserved, not rehashed modulo the new worker count. The planner supplies
/// workers in dense ID order; the first K workers retain their placement and IDs. Stateless and thread-safe.
final class MaterializedPartitionRouter {
  private static final Comparator<Worker.MaterializedPartitionHandle> HANDLE_ORDER =
      Comparator.comparingInt(Worker.MaterializedPartitionHandle::getProducerWorkerId);

  private MaterializedPartitionRouter() {
  }

  static MaterializedStageOutput snapshot(long requestId, int producerStageId, int producerCount, int partitionCount,
      List<Worker.MaterializedPartitionHandle> availableHandles) {
    boolean[][] covered = new boolean[producerCount][partitionCount];
    List<List<Worker.MaterializedPartitionHandle>> assigned = new ArrayList<>(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      assigned.add(new ArrayList<>());
    }

    int actualCount = 0;
    for (Worker.MaterializedPartitionHandle handle : availableHandles) {
      if (handle.getProducerStageId() != producerStageId) {
        continue;
      }
      actualCount++;
      if (handle.getRequestId() != requestId) {
        throw new IllegalStateException("Unexpected materialized input request id: " + handle.getRequestId());
      }
      if (handle.getByteCount() < 0 || handle.getRowCount() < 0) {
        throw new IllegalStateException("Negative materialized partition statistics: " + identity(handle));
      }
      int producerWorkerId = handle.getProducerWorkerId();
      int partitionId = handle.getLogicalPartitionId();
      if (producerWorkerId < 0 || producerWorkerId >= producerCount
          || partitionId < 0 || partitionId >= partitionCount) {
        throw new IllegalStateException("Unexpected materialized input: " + identity(handle));
      }
      if (covered[producerWorkerId][partitionId]) {
        throw new IllegalStateException("Duplicate materialized input: " + identity(handle));
      }
      covered[producerWorkerId][partitionId] = true;
      assigned.get(partitionId).add(handle);
    }

    int expectedCount = producerCount * partitionCount;
    if (actualCount != expectedCount) {
      throw new IllegalStateException("Incomplete materialized input coverage for producer stage " + producerStageId
          + ": expected=" + expectedCount + ", actual=" + actualCount);
    }

    assigned.forEach(inputs -> inputs.sort(HANDLE_ORDER));
    return new MaterializedStageOutput(producerStageId, producerCount, assigned);
  }

  static List<WorkerMetadata> route(MaterializedStageOutput output, List<WorkerMetadata> consumerWorkers,
      long targetBytes) {
    if (output.workerCount() == 0 && targetBytes > 0) {
      return List.of(consumerWorkers.get(0).withMaterializedInputs(List.of()));
    }
    List<List<Worker.MaterializedPartitionHandle>> partitions = output.partitions();
    List<List<Worker.MaterializedPartitionHandle>> groups =
        targetBytes > 0 && partitions.size() > 1 ? coalesce(partitions, targetBytes) : partitions;
    if (groups.size() > consumerWorkers.size()) {
      // Another rule may already have chosen fewer consumers. Do not undo that decision or truncate physical buckets.
      return consumerWorkers;
    }
    List<WorkerMetadata> routed = new ArrayList<>(groups.size());
    for (int workerId = 0; workerId < groups.size(); workerId++) {
      routed.add(consumerWorkers.get(workerId).withMaterializedInputs(groups.get(workerId)));
    }
    return List.copyOf(routed);
  }

  private static List<List<Worker.MaterializedPartitionHandle>> coalesce(
      List<List<Worker.MaterializedPartitionHandle>> partitions, long targetBytes) {
    List<List<Worker.MaterializedPartitionHandle>> groups = new ArrayList<>();
    List<Worker.MaterializedPartitionHandle> current = new ArrayList<>();
    long currentBytes = 0;
    for (List<Worker.MaterializedPartitionHandle> partition : partitions) {
      long bytes = 0;
      for (Worker.MaterializedPartitionHandle handle : partition) {
        bytes = Math.addExact(bytes, handle.getByteCount());
      }
      // Empty partitions attach to a neighbor, including an oversized partition. All-empty input still has one worker.
      if (currentBytes > 0 && bytes > 0 && (currentBytes >= targetBytes || bytes > targetBytes - currentBytes)) {
        groups.add(current);
        current = new ArrayList<>();
        currentBytes = 0;
      }
      current.addAll(partition);
      currentBytes = Math.addExact(currentBytes, bytes);
    }
    groups.add(current);
    return groups;
  }

  private static String identity(Worker.MaterializedPartitionHandle handle) {
    return handle.getProducerStageId() + "/" + handle.getProducerWorkerId() + "/"
        + handle.getLogicalPartitionId();
  }
}
