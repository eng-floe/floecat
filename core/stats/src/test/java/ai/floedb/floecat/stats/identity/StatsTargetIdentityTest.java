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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

class StatsTargetIdentityTest {

  @Test
  void equivalentExpressionsNormalizeToSameIdentity() {
    EngineExpressionStatsTarget a =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("  TRINO ")
            .setEngineVersion("430")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:user.id"))
            .build();
    EngineExpressionStatsTarget b =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("trino")
            .setEngineVersion("431")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:user.id"))
            .build();

    String left = StatsTargetIdentity.identityHashHex(StatsTargetIdentity.expressionTarget(a));
    String right = StatsTargetIdentity.identityHashHex(StatsTargetIdentity.expressionTarget(b));

    assertThat(left).isEqualTo(right);
  }

  @Test
  void differentExpressionsProduceDifferentIdentity() {
    EngineExpressionStatsTarget a =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("trino")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:user.id"))
            .build();
    EngineExpressionStatsTarget b =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("trino")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:event.type"))
            .build();

    assertThat(StatsTargetIdentity.expressionIdentityHashHex(a))
        .isNotEqualTo(StatsTargetIdentity.expressionIdentityHashHex(b));
  }

  @Test
  void tableTargetHasConstantHash() {
    assertThat(StatsTargetIdentity.identityHashHex(StatsTargetIdentity.tableTarget()))
        .isEqualTo("e632b7095b0bf32c260fa4c539e9fd7b852d0de454e9be26f24d0d6f91d069d3");
  }

  @Test
  void invalidExpressionTargetRejected() {
    EngineExpressionStatsTarget invalid =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind(" ")
            .setEngineExpressionKey(ByteString.copyFromUtf8("k"))
            .build();

    assertThatThrownBy(() -> StatsTargetIdentity.normalizeExpressionTarget(invalid))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("engine_kind");
  }

  @Test
  void invalidColumnTargetRejected() {
    assertThatThrownBy(
            () -> StatsTargetIdentity.identityHashHex(StatsTargetIdentity.columnTarget(0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("column_id");
  }
}
