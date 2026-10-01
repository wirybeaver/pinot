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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the spill store's round-trip, disk budget, and lifecycle contracts using real files.
public class AggregationSpillManagerTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"key", "value"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
  private Path _root;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _root = Files.createTempDirectory("aggregation-spill-test-");
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_root.toFile());
  }

  @DataProvider(name = "partitionCounts")
  public Object[][] partitionCounts() {
    return new Object[][]{{1}, {16}};
  }

  @Test(dataProvider = "partitionCounts")
  public void testRoundTripAcrossSpillRuns(int partitions) {
    List<Object[]> rows = new ArrayList<>();
    for (int key = 0; key < 10_000; key++) {
      rows.add(new Object[]{key, "value-" + key});
    }
    List<Object[]> expected = new ArrayList<>(rows);
    expected.addAll(rows.subList(0, 100));
    List<Object[]> actual = new ArrayList<>();
    Path directory;
    try (AggregationSpillManager manager = manager(partitions, "round-trip", Long.MAX_VALUE, Long.MAX_VALUE)) {
      directory = manager.getSpillDirectory();
      assertEquals(manager.spill(rows.iterator()).rows(), rows.size());
      manager.spill(rows.subList(0, 100).iterator());
      for (int partition = 0; partition < partitions; partition++) {
        manager.consumePartition(partition, block -> {
          assertTrue(block.getNumRows() <= 1024, "Records must remain bounded");
          actual.addAll(block.asRowHeap().getRows());
        });
        assertFalse(manager.hasPartition(partition));
      }
    }
    assertFalse(Files.exists(directory));
    Comparator<Object[]> byKey = Comparator.comparingInt(row -> (int) row[0]);
    actual.sort(byKey);
    expected.sort(byKey);
    assertEquals(actual.size(), expected.size());
    for (int row = 0; row < expected.size(); row++) {
      assertEquals(actual.get(row), expected.get(row));
    }
  }

  @Test
  public void testDiskBudgetsAreSharedAndReleased() {
    List<Object[]> rows = List.<Object[]>of(new Object[]{1, "value"});
    try (AggregationSpillManager first = manager(1, "query", 1024, 1024)) {
      long bytes = first.spill(rows.iterator()).bytes();
      try (AggregationSpillManager sibling = manager(1, "query", bytes, Long.MAX_VALUE);
          AggregationSpillManager otherQuery = manager(1, "other", Long.MAX_VALUE, bytes)) {
        assertDiskLimit(() -> sibling.spill(rows.iterator()));
        assertDiskLimit(() -> otherQuery.spill(rows.iterator()));

        first.consumePartition(0, block -> assertEquals(block.getNumRows(), 1));
        assertEquals(sibling.spill(rows.iterator()).bytes(), bytes,
            "Deleted partitions must release both query and server budgets before operator close");

        first.close();
        first.close();
        assertDiskLimit(() -> otherQuery.spill(rows.iterator()));
        sibling.close();
        assertEquals(otherQuery.spill(rows.iterator()).bytes(), bytes);
      }
    }
  }

  @Test
  public void testCleanupWhenConsumerFails() {
    Path directory;
    try (AggregationSpillManager manager = manager(1, "failed-consumer", 1024, 1024)) {
      directory = manager.getSpillDirectory();
      manager.spill(List.<Object[]>of(new Object[]{1, "value"}).iterator());
      IllegalStateException failure = expectThrows(IllegalStateException.class,
          () -> manager.consumePartition(0, block -> {
            throw new IllegalStateException("consumer failure");
          }));
      assertEquals(failure.getMessage(), "consumer failure");
      assertFalse(manager.hasPartition(0));
    }
    assertFalse(Files.exists(directory));
  }

  @Test
  public void testStartupSweepPreservesUnrelatedFiles()
      throws IOException {
    Path orphan = Files.createDirectory(_root.resolve("pinot-aggregation-spill-orphan"));
    Path unrelated = Files.createFile(_root.resolve("unrelated"));
    Files.createFile(orphan.resolve("partition-0.spill"));
    AggregationSpillManager.cleanOrphanedSpillFiles(_root);
    assertFalse(Files.exists(orphan));
    assertTrue(Files.exists(unrelated));
  }

  private AggregationSpillManager manager(int partitions, String query, long queryLimit, long serverLimit) {
    return new AggregationSpillManager(partitions, 1, SCHEMA, new AggregationFunction[0], _root,
        queryLimit, serverLimit, _root + "/" + query);
  }

  private static void assertDiskLimit(Runnable spill) {
    assertEquals(expectThrows(QueryException.class, spill::run).getErrorCode(),
        QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED);
  }
}
