/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.stats.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

class StatsTargetScopeCodecTest {

  @Test
  void roundTripsTableTarget() {
    StatsTarget target = StatsTargetIdentity.tableTarget();
    String encoded = StatsTargetScopeCodec.encode(target);
    assertEquals("table", encoded);
    assertEquals(target, StatsTargetScopeCodec.decode(encoded).orElseThrow());
  }

  @Test
  void roundTripsColumnTarget() {
    StatsTarget target = StatsTargetIdentity.columnTarget(42L);
    String encoded = StatsTargetScopeCodec.encode(target);
    assertEquals(target, StatsTargetScopeCodec.decode(encoded).orElseThrow());
  }

  @Test
  void roundTripsFileTarget() {
    StatsTarget target = StatsTargetIdentity.fileTarget("s3://bucket/path.parquet");
    String encoded = StatsTargetScopeCodec.encode(target);
    assertEquals(target, StatsTargetScopeCodec.decode(encoded).orElseThrow());
  }

  @Test
  void roundTripsExpressionTarget() {
    StatsTarget target =
        StatsTargetIdentity.expressionTarget(
            EngineExpressionStatsTarget.newBuilder()
                .setEngineKind("pg")
                .setEngineVersion("1")
                .setEngineExpressionKey(ByteString.copyFromUtf8("x+y"))
                .build());
    String encoded = StatsTargetScopeCodec.encode(target);
    assertEquals(target, StatsTargetScopeCodec.decode(encoded).orElseThrow());
  }

  @Test
  void returnsEmptyOnInvalid() {
    assertTrue(StatsTargetScopeCodec.decode("bogus").isEmpty());
    assertTrue(StatsTargetScopeCodec.decode("column:abc").isEmpty());
  }
}
