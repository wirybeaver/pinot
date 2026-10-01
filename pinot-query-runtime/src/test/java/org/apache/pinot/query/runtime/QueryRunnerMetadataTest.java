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
package org.apache.pinot.query.runtime;

import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests server-owned query metadata consolidation.
public class QueryRunnerMetadataTest {
  @DataProvider(name = "userMetadata")
  public Object[][] userMetadata() {
    Map<String, String> enabled = Map.of(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED, "true");
    return new Object[][]{{enabled, Map.of()}, {Map.of(), enabled}};
  }

  @Test(dataProvider = "userMetadata")
  public void testServerSpillGateOverridesUserMetadata(Map<String, String> customProperties,
      Map<String, String> requestMetadata) {
    QueryRunner queryRunner = new QueryRunner();
    Map<String, String> metadata = queryRunner.consolidateMetadata(customProperties, requestMetadata);

    assertEquals(metadata.get(QueryOptionKey.MSE_AGGREGATION_SPILL_ENABLED), "false");
  }
}
