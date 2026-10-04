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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.service.dispatch.streaming.StreamingQuerySession;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises rule composition, replacement stage graphs and completed-stage fencing through the query coordinator.
public class AdaptiveQueryExecutionTest {
  private static final long REQUEST_ID = 7;
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final QueryServerInstance SERVER = new QueryServerInstance("server", "localhost", 1234, 1235);

  @Test
  public void testCoalescingPublishesRoutingWithoutChangingCompletedProducer() {
    DispatchableSubPlan plan = plan();
    DispatchablePlanFragment producer = plan.getQueryStageMap().get(2);
    DispatchablePlanFragment oldConsumer = plan.getQueryStageMap().get(1);
    WorkerMetadata oldBroker = plan.getQueryStageMap().get(0).getWorkerMetadataList().get(0);
    StreamingQuerySession session = completedProducer();
    AdaptiveQueryExecution execution =
        new AdaptiveQueryExecution(REQUEST_ID, plan, List.of(new CoalescePartitionsRule()), Map.of());

    assertEquals(execution.nextGroup(), Set.of(2));
    execution.complete(Set.of(2), session);
    assertEquals(execution.nextGroup(), Set.of(0, 1));
    assertEquals(execution.version(), 1);
    execution.bindInputs(execution.nextGroup());

    DispatchablePlanFragment consumer = plan.getQueryStageMap().get(1);
    assertEquals(consumer.getWorkerMetadataList().size(), 1);
    assertEquals(consumer.getServerInstanceToWorkerIdMap(), Map.of(SERVER, List.of(0)));
    assertEquals(consumer.getWorkerMetadataList().get(0).getMaterializedInputs(), session.getMaterializedOutputs());
    assertEquals(plan.getQueryStageMap().get(0).getWorkerMetadataList().get(0).getMailboxInfosMap().get(1)
        .getMailboxInfos().get(0).getWorkerIds(), List.of(0));
    assertSame(plan.getQueryStageMap().get(2), producer);
    assertEquals(oldConsumer.getWorkerMetadataList().size(), 4);
    assertEquals(oldBroker.getMailboxInfosMap().get(1).getMailboxInfos().get(0).getWorkerIds(), List.of(0, 1, 2, 3));
  }

  @Test
  public void testRuleCanCallReplannerAndReplacePendingGraph() {
    DispatchableSubPlan plan = plan();
    AtomicInteger calls = new AtomicInteger();
    UnaryOperator<DispatchableSubPlan> replanner = current -> {
      calls.incrementAndGet();
      Map<Integer, DispatchablePlanFragment> stages = new HashMap<>(current.getQueryStageMap());
      stages.remove(1);
      // A different pending graph with MORE workers is allowed; it need not be a coalescing rewrite.
      stages.put(3, fragment(send(3, 0, values(3), false), 5, 0, 1));
      stages.put(0, fragment(receive(0, 3, false), 1, 3, 5));
      return current.withStageMap(stages);
    };
    AdaptiveQueryRule first = (current, context) -> {
      assertEquals(context.requestId(), REQUEST_ID);
      assertEquals(context.planVersion(), 0);
      assertEquals(context.threadBudget(), 5);
      assertEquals(context.completedStages(), Set.of(2));
      assertEquals(context.materializedOutputs().get(2).partitions().size(), 4);
      assertEquals(context.statistics().getRespondedByStage(), Map.of(2, 1));
      return replanner.apply(current);
    };
    AdaptiveQueryRule second = (current, context) -> {
      assertFalse(current.getQueryStageMap().containsKey(1));
      assertTrue(current.getQueryStageMap().containsKey(3));
      assertEquals(current.getQueryStageMap().get(3).getWorkerMetadataList().size(), 5);
      calls.incrementAndGet();
      return current;
    };
    AdaptiveQueryExecution execution = new AdaptiveQueryExecution(REQUEST_ID, plan, List.of(first, second), Map.of());

    execution.complete(Set.of(2), completedProducer());

    assertEquals(calls.get(), 2);
    assertEquals(execution.version(), 1);
    assertEquals(execution.nextGroup(), Set.of(0, 3));
    assertEquals(plan.getQueryStageMap().keySet(), Set.of(0, 2, 3));
  }

  @Test
  public void testRejectsCompletedStageRewriteBeforePublishing() {
    DispatchableSubPlan plan = plan();
    AdaptiveQueryRule invalid = (current, context) -> {
      Map<Integer, DispatchablePlanFragment> stages = new HashMap<>(current.getQueryStageMap());
      stages.put(2, fragment(values(2), 1, 1, 4));
      return current.withStageMap(stages);
    };
    AdaptiveQueryExecution execution = new AdaptiveQueryExecution(REQUEST_ID, plan, List.of(invalid), Map.of());
    assertThrows(IllegalStateException.class, () -> execution.complete(Set.of(2), completedProducer()));
    assertEquals(execution.version(), 0);
    assertTrue(plan.getQueryStageMap().get(2).getPlanFragment().getFragmentRoot() instanceof MailboxSendNode);
  }

  @Test
  public void testProducerManifestWidthIsIndependentOfConsumerParallelism() {
    DispatchableSubPlan plan = plan();
    // A previous replan chose two future consumers, but this pending writer still produces four physical buckets.
    DispatchablePlanFragment consumer = plan.getQueryStageMap().get(1);
    consumer.setWorkerMetadataList(List.copyOf(consumer.getWorkerMetadataList().subList(0, 2)));
    consumer.setServerInstanceToWorkerIdMap(Map.of(SERVER, List.of(0, 1)));
    AtomicInteger partitions = new AtomicInteger();
    AdaptiveQueryRule bind = (current, context) -> {
      partitions.set(context.materializedOutputs().get(2).partitions().size());
      return new CoalescePartitionsRule().replan(current, context);
    };
    AdaptiveQueryExecution execution =
        new AdaptiveQueryExecution(REQUEST_ID, plan, List.of(bind), Map.of("aqeTargetPartitionBytes", "20"));

    execution.complete(Set.of(2), completedProducer());
    execution.bindInputs(execution.nextGroup());

    assertEquals(partitions.get(), 4);
    List<WorkerMetadata> rebound = plan.getQueryStageMap().get(1).getWorkerMetadataList();
    assertEquals(rebound.size(), 2);
    assertEquals(rebound.get(0).getMaterializedInputs().stream()
        .map(Worker.MaterializedPartitionHandle::getLogicalPartitionId).toList(), List.of(0, 1));
    assertEquals(rebound.get(1).getMaterializedInputs().stream()
        .map(Worker.MaterializedPartitionHandle::getLogicalPartitionId).toList(), List.of(2, 3));
  }

  @Test
  public void testAggregateLimitIsKnownAndNotReachedAfterCoalescing() {
    DispatchableSubPlan plan = plan();
    DispatchablePlanFragment consumer = plan.getQueryStageMap().get(1);
    PlanNode aggregate = new AggregateNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(receive(1, 2, true)), List.of(),
        List.of(), List.of(0), AggregateNode.AggType.FINAL, false, List.of(), -1);
    Map<Integer, DispatchablePlanFragment> stages = new HashMap<>(plan.getQueryStageMap());
    stages.put(1, DispatchablePlanFragment.copyWithRoot(consumer, send(1, 0, aggregate, false)));
    DispatchableSubPlan aggregatePlan = plan.withStageMap(stages);
    CoalescePartitionsRule rule = new CoalescePartitionsRule();
    StreamingQuerySession session = completedProducer();
    MaterializedStageOutput output = MaterializedPartitionRouter.snapshot(REQUEST_ID, 2, 1, 4,
        session.getMaterializedOutputs());

    // Four rows could be four distinct groups. Unknown server limits and reaching a known cap both forbid merging.
    for (Map<String, String> options : List.of(Map.<String, String>of(), Map.of("numGroupsLimit", "4"))) {
      assertSame(rule.replan(aggregatePlan, new AdaptiveQueryContext(REQUEST_ID, 0, 5, Set.of(2), Map.of(2, output),
          session.snapshotRuntimeStats(), options)), aggregatePlan);
    }
    AdaptiveQueryContext context = new AdaptiveQueryContext(REQUEST_ID, 0, 5, Set.of(2), Map.of(2, output),
        session.snapshotRuntimeStats(), Map.of("numGroupsLimit", "5"));
    assertEquals(rule.replan(aggregatePlan, context).getQueryStageMap().get(1).getWorkerMetadataList().size(), 1);
    AdaptiveQueryContext twoGroups = new AdaptiveQueryContext(REQUEST_ID, 0, 5, Set.of(2), Map.of(2, output),
        session.snapshotRuntimeStats(), Map.of("numGroupsLimit", "3", "aqeTargetPartitionBytes", "20"));
    assertEquals(rule.replan(aggregatePlan, twoGroups).getQueryStageMap().get(1).getWorkerMetadataList().size(), 2,
        "A per-worker cap must not be applied to the sum across independent consumer workers");

    PlanNode.NodeHint hint = new PlanNode.NodeHint(Map.of(PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        Map.of(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT, "3")));
    PlanNode limited = new AggregateNode(1, SCHEMA, hint, aggregate.getInputs(), List.of(), List.of(), List.of(0),
        AggregateNode.AggType.FINAL, false, List.of(), -1);
    stages.put(1, DispatchablePlanFragment.copyWithRoot(consumer, send(1, 0, limited, false)));
    DispatchableSubPlan hinted = plan.withStageMap(stages);
    assertSame(rule.replan(hinted, context), hinted, "An aggregate hint overrides the larger query cap");
  }

  @Test
  public void testRejectsGroupThatExceedsOriginalAdmissionBudget() {
    DispatchableSubPlan plan = plan();
    AdaptiveQueryRule invalid = (current, context) -> {
      Map<Integer, DispatchablePlanFragment> stages = new HashMap<>(current.getQueryStageMap());
      stages.put(1, fragment(send(1, 0, values(1), false), 6, 0, 1));
      stages.put(0, fragment(receive(0, 1, false), 1, 1, 6));
      return current.withStageMap(stages);
    };
    AdaptiveQueryExecution execution = new AdaptiveQueryExecution(REQUEST_ID, plan, List.of(invalid), Map.of());
    assertThrows(IllegalStateException.class, () -> execution.complete(Set.of(2), completedProducer()));
    assertEquals(plan.getQueryStageMap().get(1).getWorkerMetadataList().size(), 4);
  }

  @Test
  public void testPartitionSensitiveOperatorsRemainUnchanged() {
    DispatchableSubPlan plan = plan();
    List<PlanNode> inputs = List.of(receive(1, 2, true));
    StreamingQuerySession session = completedProducer();
    MaterializedStageOutput output = MaterializedPartitionRouter.snapshot(REQUEST_ID, 2, 1, 4,
        session.getMaterializedOutputs());
    AdaptiveQueryContext context = new AdaptiveQueryContext(REQUEST_ID, 0, 5, Set.of(2), Map.of(2, output),
        session.snapshotRuntimeStats(), Map.of("numGroupsLimit", "5"));
    List<PlanNode> sensitive = List.of(
        new SortNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, inputs, List.of(), 3, 0),
        new SortNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, inputs, List.of(), -1, 1),
        new AggregateNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, inputs, List.of(), List.of(), List.of(0),
            AggregateNode.AggType.FINAL, false, List.of(), 3),
        new AggregateNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, inputs, List.of(), List.of(), List.of(0),
            AggregateNode.AggType.DIRECT, false, List.of(), -1, List.of(List.of(0), List.of())));
    for (PlanNode node : sensitive) {
      Map<Integer, DispatchablePlanFragment> stages = new HashMap<>(plan.getQueryStageMap());
      stages.put(1, DispatchablePlanFragment.copyWithRoot(stages.get(1), send(1, 0, node, false)));
      DispatchableSubPlan candidate = plan.withStageMap(stages);
      assertSame(new CoalescePartitionsRule().replan(candidate, context), candidate, node.explain());
    }
  }

  private static DispatchableSubPlan plan() {
    return new DispatchableSubPlan(PairList.of(0, "col"), Map.of(
        2, fragment(send(2, 1, values(2), true), 1, 1, 4),
        1, fragment(send(1, 0, receive(1, 2, true), false), 4, 0, 1),
        0, fragment(receive(0, 1, false), 1, 1, 4)), Set.of(), Map.of(), 0);
  }

  private static StreamingQuerySession completedProducer() {
    StreamingQuerySession session = new StreamingQuerySession(REQUEST_ID, 1, Map.of(2, Set.of(0)));
    List<Worker.MaterializedPartitionHandle> handles = IntStream.range(0, 4)
        .mapToObj(partition -> Worker.MaterializedPartitionHandle.newBuilder().setRequestId(REQUEST_ID)
            .setProducerStageId(2).setProducerWorkerId(0).setLogicalPartitionId(partition)
            .setHost("localhost").setTransferPort(1235).setByteCount(10).setRowCount(1).build()).toList();
    session.recordOpChainComplete(Worker.OpChainComplete.newBuilder().setStageId(2).setWorkerId(0).setSuccess(true)
        .addAllMaterializedOutput(handles).build());
    return session;
  }

  private static DispatchablePlanFragment fragment(PlanNode root, int workers, int peerStage, int peerWorkers) {
    List<Integer> peerIds = IntStream.range(0, peerWorkers).boxed().toList();
    Map<Integer, MailboxInfos> mailboxes =
        Map.of(peerStage, new MailboxInfos(new MailboxInfo("localhost", 1235, peerIds)));
    DispatchablePlanFragment fragment =
        new DispatchablePlanFragment(new PlanFragment(root.getStageId(), root, List.of()));
    fragment.setWorkerMetadataList(IntStream.range(0, workers)
        .mapToObj(id -> new WorkerMetadata(id, mailboxes, Map.of())).toList());
    fragment.setServerInstanceToWorkerIdMap(Map.of(SERVER, IntStream.range(0, workers).boxed().toList()));
    return fragment;
  }

  private static PlanNode values(int stage) {
    return new ValueNode(stage, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of());
  }

  private static PlanNode send(int stage, int receiver, PlanNode input, boolean materialized) {
    return new MailboxSendNode(stage, SCHEMA, List.of(input), List.of(receiver), PinotRelExchangeType.STREAMING,
        materialized ? RelDistribution.Type.HASH_DISTRIBUTED : RelDistribution.Type.SINGLETON, List.of(0), false,
        List.of(), false, "MURMUR3", materialized);
  }

  private static PlanNode receive(int stage, int sender, boolean materialized) {
    return new MailboxReceiveNode(stage, SCHEMA, sender, PinotRelExchangeType.STREAMING,
        materialized ? RelDistribution.Type.HASH_DISTRIBUTED : RelDistribution.Type.SINGLETON, List.of(), List.of(),
        false, false, null, materialized);
  }
}
