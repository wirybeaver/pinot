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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Tests deterministic late binding of materialized partitions.
public class MaterializedPartitionRouterTest {
  private static final long REQUEST_ID = 7L;
  private static final int PRODUCER_STAGE_ID = 1;

  @Test
  public void testRoutesEveryProducerPartitionToMatchingConsumer() {
    Map<Integer, MailboxInfos> mailboxes = Map.of(9,
        new MailboxInfos(new MailboxInfo("consumer", 1234, List.of(0))));
    List<WorkerMetadata> consumerWorkers = List.of(
        new WorkerMetadata(0, mailboxes, Map.of("property", "value")), new WorkerMetadata(1, Map.of()));
    Map<String, List<String>> tableSegments = Map.of("OFFLINE", List.of("segment"));
    Map<String, List<String>> logicalTableSegments = Map.of("table_OFFLINE", List.of("logical-segment"));
    consumerWorkers.get(0).setTableSegmentsMap(tableSegments);
    consumerWorkers.get(1).setLogicalTableSegmentsMap(logicalTableSegments);
    List<Worker.MaterializedPartitionHandle> handles = List.of(
        handle(2, 1), handle(1, 0), handle(0, 1), handle(2, 0), handle(0, 0), handle(1, 1));

    MaterializedStageOutput output = MaterializedPartitionRouter.snapshot(REQUEST_ID, PRODUCER_STAGE_ID, 3, 2, handles);
    List<WorkerMetadata> routed = MaterializedPartitionRouter.route(output, consumerWorkers, 0);

    assertEquals(routed.size(), 2);
    assertEquals(routed.get(0).getWorkerId(), 0);
    assertEquals(routed.get(0).getMaterializedInputs(), List.of(handle(0, 0), handle(1, 0), handle(2, 0)));
    assertEquals(routed.get(1).getWorkerId(), 1);
    assertEquals(routed.get(1).getMaterializedInputs(), List.of(handle(0, 1), handle(1, 1), handle(2, 1)));
    assertEquals(routed.get(0).getMailboxInfosMap(), mailboxes);
    assertEquals(routed.get(0).getCustomProperties(), Map.of("property", "value"));
    assertEquals(routed.get(0).getTableSegmentsMap(), tableSegments);
    assertEquals(routed.get(1).getLogicalTableSegmentsMap(), logicalTableSegments);
  }

  @Test
  public void testRejectsMissingDuplicateAndUnexpectedCoverage() {
    List<Worker.MaterializedPartitionHandle> complete =
        List.of(handle(0, 0), handle(0, 1), handle(1, 0), handle(1, 1));

    assertThrows(IllegalStateException.class, () -> MaterializedPartitionRouter.snapshot(
        REQUEST_ID, PRODUCER_STAGE_ID, 2, 2, complete.subList(0, 3)));
    assertThrows(IllegalStateException.class, () -> MaterializedPartitionRouter.snapshot(
        REQUEST_ID, PRODUCER_STAGE_ID, 2, 2,
        List.of(handle(0, 0), handle(0, 0), handle(1, 0), handle(1, 1))));
    assertThrows(IllegalStateException.class, () -> MaterializedPartitionRouter.snapshot(
        REQUEST_ID, PRODUCER_STAGE_ID, 2, 2,
        List.of(handle(0, 0), handle(0, 1), handle(1, 0), handle(1, 2))));
    assertThrows(IllegalStateException.class, () -> MaterializedPartitionRouter.snapshot(
        REQUEST_ID, PRODUCER_STAGE_ID, 2, 2,
        List.of(handle(0, 0).toBuilder().setRequestId(8L).build(), handle(0, 1), handle(1, 0), handle(1, 1))));
  }

  @Test
  public void testEmptyProducerHasNoInputs() {
    MaterializedStageOutput output =
        MaterializedPartitionRouter.snapshot(REQUEST_ID, PRODUCER_STAGE_ID, 0, 1, List.of());
    List<WorkerMetadata> routed = MaterializedPartitionRouter.route(output, workers(1), 0);
    assertEquals(routed.get(0).getMaterializedInputs(), List.of());
  }

  @Test
  public void testCoalescesByBytesAcrossProducersWithoutRehashing() {
    // Total bucket sizes: [40, 60, 0, 140, 0, 20]. Exact target fits; oversized buckets stay intact.
    long[] bytes = {20, 30, 0, 70, 0, 10};
    List<Worker.MaterializedPartitionHandle> handles = new ArrayList<>();
    for (int partition = 0; partition < bytes.length; partition++) {
      for (int producer = 0; producer < 2; producer++) {
        handles.add(handle(producer, partition).toBuilder().setByteCount(bytes[partition]).build());
      }
    }
    List<WorkerMetadata> workers = workers(bytes.length);
    MaterializedStageOutput output =
        MaterializedPartitionRouter.snapshot(REQUEST_ID, PRODUCER_STAGE_ID, 2, bytes.length, handles);
    List<WorkerMetadata> routed = MaterializedPartitionRouter.route(output, workers, 100);

    assertEquals(routed.size(), 3);
    assertEquals(routed.get(0).getMaterializedInputs(), handles.subList(0, 6));
    assertEquals(routed.get(1).getMaterializedInputs(), handles.subList(6, 10));
    assertEquals(routed.get(2).getMaterializedInputs(), handles.subList(10, 12));
    Collections.reverse(handles);
    List<WorkerMetadata> reordered = MaterializedPartitionRouter.route(
        MaterializedPartitionRouter.snapshot(REQUEST_ID, PRODUCER_STAGE_ID, 2, bytes.length, handles), workers, 100);
    for (int i = 0; i < routed.size(); i++) {
      assertEquals(reordered.get(i).getWorkerId(), i);
      assertEquals(reordered.get(i).getMaterializedInputs(), routed.get(i).getMaterializedInputs());
    }
    assertEquals(MaterializedPartitionRouter.route(output, workers, 0).size(), bytes.length);
  }

  @Test
  public void testEmptyBucketsCoalesceToOneWorker() {
    List<Worker.MaterializedPartitionHandle> handles = List.of(handle(0, 0), handle(0, 1), handle(0, 2));
    MaterializedStageOutput output = MaterializedPartitionRouter.snapshot(REQUEST_ID, PRODUCER_STAGE_ID, 1, 3, handles);

    List<WorkerMetadata> routed = MaterializedPartitionRouter.route(output, workers(3), 100);

    assertEquals(routed.size(), 1);
    assertEquals(routed.get(0).getMaterializedInputs(), handles);
  }

  private static List<WorkerMetadata> workers(int count) {
    return IntStream.range(0, count)
        .mapToObj(workerId -> new WorkerMetadata(workerId, Map.of(), Map.of()))
        .toList();
  }

  private static Worker.MaterializedPartitionHandle handle(int producerWorkerId, int logicalPartitionId) {
    return Worker.MaterializedPartitionHandle.newBuilder()
        .setRequestId(REQUEST_ID)
        .setProducerStageId(PRODUCER_STAGE_ID)
        .setProducerWorkerId(producerWorkerId)
        .setLogicalPartitionId(logicalPartitionId)
        .setHost("producer-" + producerWorkerId)
        .setTransferPort(1000 + producerWorkerId)
        .setRowCount(0)
        .setByteCount(0)
        .build();
  }
}
