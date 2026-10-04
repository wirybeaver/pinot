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

import org.apache.pinot.query.planner.physical.DispatchableSubPlan;


/// One ordered physical-replanning rule, invoked after a dispatch group completes and before the next is submitted.
/// Rules may invoke another planner and return a replacement dispatchable plan, or return the input for no change.
/// They must not mutate the input plan, must preserve completed stages and the query output schema, and must supply
/// correct worker/mailbox routing for their rewrites. Stateful implementations must be safe for concurrent queries.
/// The final pipeline output must fit each dispatch group within the original thread budget. Custom broker thread
/// estimators need corresponding adaptive admission integration before installing rules that grow work.
@FunctionalInterface
public interface AdaptiveQueryRule {
  DispatchableSubPlan replan(DispatchableSubPlan plan, AdaptiveQueryContext context);
}
