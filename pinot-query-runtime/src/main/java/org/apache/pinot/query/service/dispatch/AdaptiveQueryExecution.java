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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.QueryPlanSerDeUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.service.dispatch.streaming.StreamingQuerySession;


/// Per-query, broker-thread-owned adaptive lifecycle: complete group -> snapshot -> ordered rules -> publish plan.
/// Only unsubmitted work may change. Replanning can replace pending operators, stages, routing and parallelism; rule-
/// specific eligibility stays in each rule. No worker is resubmitted and completed output identities never change.
final class AdaptiveQueryExecution {
  private final long _requestId;
  private final DispatchableSubPlan _plan;
  private final List<AdaptiveQueryRule> _rules;
  private final Map<String, String> _options;
  private final int _threadBudget;
  private final Set<Integer> _completed = new HashSet<>();
  private final Map<Integer, StageSnapshot> _frozen = new HashMap<>();
  private final Map<Integer, MaterializedStageOutput> _outputs = new HashMap<>();
  private List<Set<Integer>> _groups;
  private int _version;

  AdaptiveQueryExecution(long requestId, DispatchableSubPlan plan, List<AdaptiveQueryRule> rules,
      Map<String, String> options) {
    _requestId = requestId;
    _plan = plan;
    _rules = List.copyOf(rules);
    _options = Map.copyOf(options);
    _threadBudget = plan.getEstimatedNumQueryThreads();
    _groups = StageDispatchGraph.create(plan);
  }

  int version() {
    return _version;
  }

  /// Returns the next dependency-ready group from the latest published graph, never from a stale pre-replan order.
  Set<Integer> nextGroup() {
    for (Set<Integer> group : _groups) {
      if (!_completed.containsAll(group)) {
        return group;
      }
    }
    return Set.of();
  }

  /// Binds ordinary unassigned inputs. A rule that changes the partition-to-worker mapping supplies its own handles.
  /// Multiple inputs are supported here; alignment and join semantics are the rewriting rule's responsibility.
  void bindInputs(Set<Integer> group) {
    for (int stageId : group) {
      DispatchablePlanFragment stage = _plan.getQueryStageMap().get(stageId);
      Set<Integer> producers = new HashSet<>();
      collectMaterializedInputs(stage.getPlanFragment().getFragmentRoot(), producers);
      List<WorkerMetadata> workers = new ArrayList<>(stage.getWorkerMetadataList());
      for (int producerId : new TreeSet<>(producers)) {
        MaterializedStageOutput output = _outputs.get(producerId);
        if (output == null) {
          throw new IllegalStateException("Materialized producer is not complete: " + producerId);
        }
        if (workers.stream().anyMatch(worker -> worker.getMaterializedInputs().stream()
            .anyMatch(handle -> handle.getProducerStageId() == producerId))) {
          continue;
        }
        if (workers.size() != output.partitions().size() && output.workerCount() != 0) {
          throw new IllegalStateException("Replanned stage " + stageId + " needs explicit materialized input routing");
        }
        for (int workerId = 0; workerId < workers.size(); workerId++) {
          WorkerMetadata worker = workers.get(workerId);
          List<Worker.MaterializedPartitionHandle> handles = new ArrayList<>(worker.getMaterializedInputs());
          if (output.workerCount() != 0) {
            handles.addAll(output.partitions().get(workerId));
          }
          workers.set(workerId, worker.withMaterializedInputs(handles));
        }
      }
      if (!producers.isEmpty()) {
        stage.setWorkerMetadataList(workers);
      }
    }
  }

  /// Called only after successful worker coverage and terminal stream completion for this group.
  void complete(Set<Integer> group, StreamingQuerySession session) {
    _completed.addAll(group);
    if (_completed.containsAll(_plan.getQueryStageMap().keySet())) {
      return;
    }
    List<Worker.MaterializedPartitionHandle> outputs = session.getMaterializedOutputs();
    for (int stageId : group) {
      DispatchablePlanFragment stage = _plan.getQueryStageMap().get(stageId);
      if (!_rules.isEmpty()) {
        _frozen.put(stageId, snapshot(stage));
      }
      PlanNode root = stage.getPlanFragment().getFragmentRoot();
      if (root instanceof MailboxSendNode && ((MailboxSendNode) root).isMaterialized()) {
        int receiver = ((MailboxSendNode) root).getReceiverStageIds().iterator().next();
        // The dispatched writer's layout owns physical bucket identity, not the current consumer's parallelism.
        int partitions = stage.getWorkerMetadataList().isEmpty() ? 0
            : stage.getWorkerMetadataList().get(0).getMailboxInfosMap().get(receiver).getMailboxInfos().stream()
                .mapToInt(info -> info.getWorkerIds().size()).sum();
        _outputs.put(stageId, MaterializedPartitionRouter.snapshot(_requestId, stageId,
            stage.getWorkerMetadataList().size(), partitions, outputs));
      }
    }
    if (_rules.isEmpty()) {
      return;
    }
    AdaptiveQueryContext context = new AdaptiveQueryContext(_requestId, _version, _threadBudget, _completed, _outputs,
        session.snapshotRuntimeStats(), _options);
    DispatchableSubPlan candidate = _plan;
    for (AdaptiveQueryRule rule : _rules) {
      candidate = rule.replan(candidate, context);
      validateCompletedStages(candidate);
    }
    if (candidate != _plan) {
      List<Set<Integer>> groups = validateRewrite(candidate);
      _plan.replaceStageMap(candidate.getQueryStageMap());
      _groups = groups;
      _version++;
    }
  }

  private void validateCompletedStages(DispatchableSubPlan candidate) {
    for (Map.Entry<Integer, StageSnapshot> entry : _frozen.entrySet()) {
      DispatchablePlanFragment stage = candidate.getQueryStageMap().get(entry.getKey());
      if (stage == null || !entry.getValue().equals(snapshot(stage))) {
        throw new IllegalStateException("Adaptive rule changed completed stage " + entry.getKey());
      }
    }
  }

  private List<Set<Integer>> validateRewrite(DispatchableSubPlan candidate) {
    DispatchablePlanFragment root = candidate.getQueryStageMap().get(0);
    if (root == null || root.getWorkerMetadataList().size() != 1
        || !candidate.getQueryResultFields().equals(_plan.getQueryResultFields())
        || !root.getPlanFragment().getFragmentRoot().getDataSchema()
            .equals(_plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema())) {
      throw new IllegalStateException("Adaptive rule changed the query output contract");
    }
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : candidate.getQueryStageMap().entrySet()) {
      DispatchablePlanFragment stage = entry.getValue();
      int stageId = stage.getPlanFragment().getFragmentId();
      if (entry.getKey() != stageId) {
        throw new IllegalStateException("Stage map key does not match fragment " + stageId);
      }
      if (stageId == 0) {
        continue;
      }
      if (_completed.contains(stageId)) {
        continue;
      }
      PlanNode stageRoot = stage.getPlanFragment().getFragmentRoot();
      if (stageRoot instanceof MailboxSendNode) {
        for (int receiverId : ((MailboxSendNode) stageRoot).getReceiverStageIds()) {
          if (_completed.contains(receiverId) || !candidate.getQueryStageMap().containsKey(receiverId)) {
            throw new IllegalStateException("Pending stage " + stageId + " has unavailable receiver " + receiverId);
          }
        }
      }
      Set<Integer> placed = new HashSet<>();
      stage.getServerInstanceToWorkerIdMap().values().forEach(ids -> {
        for (int id : ids) {
          if (!placed.add(id)) {
            throw new IllegalStateException("Duplicate worker placement in stage " + stageId);
          }
        }
      });
      List<WorkerMetadata> workers = stage.getWorkerMetadataList();
      if (placed.size() != workers.size()) {
        throw new IllegalStateException("Incomplete worker placement in stage " + stageId);
      }
      for (int i = 0; i < workers.size(); i++) {
        if (workers.get(i).getWorkerId() != i || !placed.contains(i)) {
          throw new IllegalStateException("Non-dense worker placement in stage " + stageId);
        }
      }
    }
    List<Set<Integer>> groups = StageDispatchGraph.create(candidate);
    for (Set<Integer> group : groups) {
      if (group.stream().anyMatch(_completed::contains) && !_completed.containsAll(group)) {
        throw new IllegalStateException("Adaptive rule connected pending work to a completed streaming group");
      }
      if (!_completed.containsAll(group)) {
        Map<Integer, DispatchablePlanFragment> groupStages = new HashMap<>();
        group.forEach(id -> groupStages.put(id, candidate.getQueryStageMap().get(id)));
        int threads = candidate.withStageMap(groupStages).getEstimatedNumQueryThreads();
        if (threads > _threadBudget) {
          throw new IllegalStateException("Adaptive group " + group + " needs " + threads
              + " estimated threads, exceeding the original admission budget " + _threadBudget);
        }
      }
    }
    return groups;
  }

  private static void collectMaterializedInputs(PlanNode node, Set<Integer> producers) {
    if (node instanceof MailboxReceiveNode && ((MailboxReceiveNode) node).isMaterialized()) {
      producers.add(((MailboxReceiveNode) node).getSenderStageId());
    }
    for (PlanNode input : node.getInputs()) {
      collectMaterializedInputs(input, producers);
    }
  }

  private static StageSnapshot snapshot(DispatchablePlanFragment stage) {
    Worker.StagePlan wirePlan = Worker.StagePlan.newBuilder()
        .setRootNode(PlanNodeSerializer.process(stage.getPlanFragment().getFragmentRoot()).toByteString())
        .setStageMetadata(Worker.StageMetadata.newBuilder().setStageId(stage.getPlanFragment().getFragmentId())
            .setCustomProperty(QueryPlanSerDeUtils.toProtoProperties(stage.getCustomProperties()))
            .addAllWorkerMetadata(QueryPlanSerDeUtils.toProtoWorkerMetadataList(stage.getWorkerMetadataList())))
        .build();
    Map<QueryServerInstance, List<Integer>> placement = new HashMap<>();
    stage.getServerInstanceToWorkerIdMap().forEach((server, ids) -> placement.put(server, List.copyOf(ids)));
    return new StageSnapshot(wirePlan, Map.copyOf(placement));
  }

  private record StageSnapshot(Worker.StagePlan wirePlan, Map<QueryServerInstance, List<Integer>> placement) {
  }
}
