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

import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Tests the deterministic adaptive partition-coalescing policy.
public class AdaptivePartitionCoalescerTest {
  @Test
  public void shouldCoalesceSmallPartitionsIntoContiguousGroups() {
    List<AdaptivePartitionCoalescer.PartitionGroup> groups =
        AdaptivePartitionCoalescer.coalesce(new long[]{50, 50, 50, 50, 50, 50, 50, 50}, 200);
    assertEquals(groups,
        List.of(new AdaptivePartitionCoalescer.PartitionGroup(0, 4),
            new AdaptivePartitionCoalescer.PartitionGroup(4, 8)));
    expectThrows(UnsupportedOperationException.class,
        () -> groups.add(new AdaptivePartitionCoalescer.PartitionGroup(0, 8)));
  }

  @Test
  public void shouldKeepOriginalAssignmentWhenParallelismCannotDecrease() {
    assertEquals(AdaptivePartitionCoalescer.coalesce(new long[]{300, 300}, 200),
        List.of(new AdaptivePartitionCoalescer.PartitionGroup(0, 1),
            new AdaptivePartitionCoalescer.PartitionGroup(1, 2)));
    assertEquals(AdaptivePartitionCoalescer.coalesce(new long[]{300, 1}, 200),
        List.of(new AdaptivePartitionCoalescer.PartitionGroup(0, 1),
            new AdaptivePartitionCoalescer.PartitionGroup(1, 2)));
  }

  @Test
  public void shouldFoldSmallTrailingGroupIntoItsPredecessor() {
    assertEquals(AdaptivePartitionCoalescer.coalesce(new long[]{1_000, 50}, 1_024),
        List.of(new AdaptivePartitionCoalescer.PartitionGroup(0, 2)));
  }

  @Test
  public void shouldNotOverflowIntermediateGroupSizes() {
    assertEquals(AdaptivePartitionCoalescer.coalesce(
            new long[]{1_500_000_000_000_000_000L, 7_800_000_000_000_000_000L, 1_600_000_000_000_000_000L},
            8_000_000_000_000_000_000L),
        List.of(new AdaptivePartitionCoalescer.PartitionGroup(0, 2),
            new AdaptivePartitionCoalescer.PartitionGroup(2, 3)));
  }

  @Test
  public void shouldHandleEmptyAndSinglePartitionInputs() {
    assertEquals(AdaptivePartitionCoalescer.coalesce(new long[]{}, 200), List.of());
    assertEquals(AdaptivePartitionCoalescer.coalesce(new long[]{500}, 200),
        List.of(new AdaptivePartitionCoalescer.PartitionGroup(0, 1)));
  }

  @Test
  public void shouldRejectInvalidMeasurements() {
    expectThrows(IllegalArgumentException.class,
        () -> AdaptivePartitionCoalescer.coalesce(new long[]{50}, 0));
    expectThrows(IllegalArgumentException.class,
        () -> AdaptivePartitionCoalescer.coalesce(new long[]{50, -1}, 200));
  }
}
