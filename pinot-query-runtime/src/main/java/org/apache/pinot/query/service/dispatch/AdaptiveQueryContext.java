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

import java.util.Map;
import java.util.Set;
import org.apache.pinot.query.service.dispatch.streaming.StreamingQuerySession;


/// Runtime snapshot for one rule pipeline. The version identifies the published plan that produced these statistics,
/// not a wire-protocol version or retry attempt. Statistics are detached from the live session; rules treat them as
/// read-only. Completed output identities remain fixed even when a rule replaces the remaining stage graph.
/// The thread budget uses Pinot's default estimator and bounds each concurrently dispatched group.
public record AdaptiveQueryContext(long requestId, int planVersion, int threadBudget, Set<Integer> completedStages,
                                   Map<Integer, MaterializedStageOutput> materializedOutputs,
                                   StreamingQuerySession.Coverage statistics, Map<String, String> queryOptions) {
  public AdaptiveQueryContext {
    completedStages = Set.copyOf(completedStages);
    materializedOutputs = Map.copyOf(materializedOutputs);
    queryOptions = Map.copyOf(queryOptions);
  }
}
