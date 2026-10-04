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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;


/// Rewrites only a not-yet-dispatched unary materialized consumer that gathers directly into the singleton broker.
/// Joins and other multi-input stages require aligned partition mappings and are intentionally left unchanged.
/// These restrictions belong to this rule, not the adaptive framework. Stateless and thread-safe; input plans are
/// never mutated. Other rules may supply different physical replanners and routing strategies.
public final class CoalescePartitionsRule implements AdaptiveQueryRule {
  @Override
  public DispatchableSubPlan replan(DispatchableSubPlan plan, AdaptiveQueryContext context) {
    long targetBytes = QueryOptionsUtils.getAqeTargetPartitionBytes(context.queryOptions());
    if (targetBytes == 0) {
      return plan;
    }
    Map<Integer, DispatchablePlanFragment> stages = new HashMap<>(plan.getQueryStageMap());
    boolean changed = false;
    for (DispatchablePlanFragment consumer : plan.getQueryStagesWithoutRoot()) {
      int stageId = consumer.getPlanFragment().getFragmentId();
      DispatchablePlanFragment broker = stages.get(0);
      if (context.completedStages().contains(stageId) || context.completedStages().contains(0)
          || !isEligible(consumer, broker)
          || consumer.getWorkerMetadataList().stream().anyMatch(worker -> !worker.getMaterializedInputs().isEmpty())) {
        continue;
      }
      PlanNode input = consumer.getPlanFragment().getFragmentRoot();
      while (!(input instanceof MailboxReceiveNode)) {
        input = input.getInputs().get(0);
      }
      MaterializedStageOutput output =
          context.materializedOutputs().get(((MailboxReceiveNode) input).getSenderStageId());
      if (output == null) {
        continue;
      }
      List<WorkerMetadata> workers =
          MaterializedPartitionRouter.route(output, consumer.getWorkerMetadataList(), targetBytes);
      if (workers == consumer.getWorkerMetadataList()
          || workers.size() == consumer.getWorkerMetadataList().size() && workers.size() == output.partitions().size()
          || !withinOperatorLimits(consumer, workers, context.queryOptions())) {
        continue;
      }
      DispatchablePlanFragment replacement = DispatchablePlanFragment.copyWithRoot(consumer,
          consumer.getPlanFragment().getFragmentRoot());
      DispatchablePlanFragment newBroker = DispatchablePlanFragment.copyWithRoot(broker,
          broker.getPlanFragment().getFragmentRoot());
      resize(replacement, newBroker, workers);
      stages.put(stageId, replacement);
      stages.put(0, newBroker);
      changed = true;
    }
    return changed ? plan.withStageMap(stages) : plan;
  }

  private static boolean isEligible(DispatchablePlanFragment consumer, DispatchablePlanFragment broker) {
    if (consumer.getWorkerMetadataList().size() <= 1 || broker.getWorkerMetadataList().size() != 1
        || consumer.getWorkerMetadataList().stream().anyMatch(WorkerMetadata::isLeafStageWorker)
        || !(consumer.getPlanFragment().getFragmentRoot() instanceof MailboxSendNode)) {
      return false;
    }
    MailboxSendNode send = (MailboxSendNode) consumer.getPlanFragment().getFragmentRoot();
    if (send.isMultiSend() || send.isMaterialized() || send.getReceiverStageIds().iterator().next() != 0) {
      return false;
    }
    PlanNode node = send.getInputs().get(0);
    while (node instanceof AggregateNode || node instanceof ProjectNode || node instanceof FilterNode
        || node instanceof SortNode) {
      if (node.getInputs().size() != 1
          || node instanceof SortNode && ((SortNode) node).getOffset() > 0
          || node instanceof AggregateNode
          && (((AggregateNode) node).getGroupKeys().isEmpty() || ((AggregateNode) node).isGroupingSets())) {
        return false;
      }
      node = node.getInputs().get(0);
    }
    if (!(node instanceof MailboxReceiveNode)) {
      return false;
    }
    MailboxReceiveNode receive = (MailboxReceiveNode) node;
    return receive.isMaterialized() && receive.getDistributionType() == RelDistribution.Type.HASH_DISTRIBUTED
        && !receive.isSort() && !receive.isSortedOnSender();
  }

  /// Per-worker input rows bound groups and local limits in the unary chain. Never override server resource caps:
  /// nonempty aggregates require an explicit group cap (hint > stage metadata > query option) before dispatch.
  private static boolean withinOperatorLimits(DispatchablePlanFragment consumer, List<WorkerMetadata> workers,
      Map<String, String> options) {
    long rows = 0;
    for (WorkerMetadata worker : workers) {
      long workerRows = 0;
      for (var handle : worker.getMaterializedInputs()) {
        workerRows = Math.addExact(workerRows, handle.getRowCount());
      }
      rows = Math.max(rows, workerRows);
    }
    PlanNode node = consumer.getPlanFragment().getFragmentRoot().getInputs().get(0);
    while (!(node instanceof MailboxReceiveNode)) {
      // Keep local top-N and aggregate trimming nonbinding without assuming a particular global operator shape.
      int localLimit = node instanceof SortNode ? ((SortNode) node).getFetch()
          : node instanceof AggregateNode ? ((AggregateNode) node).getLimit() : -1;
      if (localLimit > 0 && rows > localLimit) {
        return false;
      }
      if (node instanceof AggregateNode && rows > 0) {
        String limit = node.getNodeHint().getHintOptions()
            .getOrDefault(PinotHintOptions.AGGREGATE_HINT_OPTIONS, Map.of())
            .get(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT);
        if (limit == null) {
          limit = consumer.getCustomProperties().getOrDefault(QueryOptionKey.NUM_GROUPS_LIMIT,
              options.get(QueryOptionKey.NUM_GROUPS_LIMIT));
        }
        // Reaching the limit exactly also raises NUM_GROUPS_LIMIT_REACHED in AggregateOperator.
        if (limit == null || rows >= Integer.parseInt(limit)) {
          return false;
        }
      }
      node = node.getInputs().get(0);
    }
    return true;
  }

  /// Keeps the first K worker placements and replaces the broker's sender list with exactly those K worker IDs.
  /// Construct replacement metadata before publishing it; never mutate cached/shared MailboxInfos in place.
  private static void resize(DispatchablePlanFragment consumer, DispatchablePlanFragment broker,
      List<WorkerMetadata> workers) {
    int workerCount = workers.size();
    int consumerStageId = consumer.getPlanFragment().getFragmentId();
    Map<QueryServerInstance, List<Integer>> placement = new HashMap<>();
    consumer.getServerInstanceToWorkerIdMap().forEach((server, ids) -> {
      List<Integer> retained = ids.stream().filter(id -> id < workerCount).toList();
      if (!retained.isEmpty()) {
        placement.put(server, retained);
      }
    });
    WorkerMetadata brokerWorker = broker.getWorkerMetadataList().get(0);
    Map<Integer, MailboxInfos> mailboxes = new HashMap<>(brokerWorker.getMailboxInfosMap());
    List<MailboxInfo> senders = new ArrayList<>();
    for (MailboxInfo info : mailboxes.get(consumerStageId).getMailboxInfos()) {
      List<Integer> ids = info.getWorkerIds().stream().filter(id -> id < workerCount).toList();
      if (!ids.isEmpty()) {
        senders.add(new MailboxInfo(info.getHostname(), info.getPort(), ids));
      }
    }
    mailboxes.put(consumerStageId, new MailboxInfos(senders));
    WorkerMetadata reboundBroker = brokerWorker.withMailboxInfos(mailboxes);

    consumer.setWorkerMetadataList(workers);
    consumer.setServerInstanceToWorkerIdMap(placement);
    broker.setWorkerMetadataList(List.of(reboundBroker));
  }
}
