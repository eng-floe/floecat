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

package ai.floedb.floecat.extensions.floedb.engine.oid;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import org.junit.jupiter.api.Test;

class DefaultEngineOidGeneratorTest {

  private static final ResourceId BASE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("test-table")
          .build();

  @Test
  void oidsAreAlwaysAboveMinimum() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    for (int i = 0; i < 200_000; i++) {
      ResourceId id =
          ResourceId.newBuilder()
              .setAccountId("acct")
              .setKind(ResourceKind.RK_TABLE)
              .setId("t-" + i)
              .build();
      int oid = generator.generate(id, "floe.relation+proto");
      assertThat(oid).isGreaterThanOrEqualTo(DefaultEngineOidGenerator.MIN_OID);
    }
  }

  @Test
  void deterministicForSameInputs() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    int first = generator.generate(BASE_ID, "floe.relation+proto");
    int repeat = generator.generate(BASE_ID, "floe.relation+proto");
    assertThat(repeat).isEqualTo(first);
  }

  @Test
  void deterministicForSameInputsAndSameSalt() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    int first = generator.generate(BASE_ID, "floe.relation+proto", "v1");
    int repeat = generator.generate(BASE_ID, "floe.relation+proto", "v1");
    assertThat(repeat).isEqualTo(first);
  }

  @Test
  void differsForDifferentSalts() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    int v1 = generator.generate(BASE_ID, "floe.relation+proto", "v1");
    int v2 = generator.generate(BASE_ID, "floe.relation+proto", "v2");
    assertThat(v2).isNotEqualTo(v1);
  }

  @Test
  void differsForDifferentPayloads() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    int relation = generator.generate(BASE_ID, "floe.relation+proto");
    int column = generator.generate(BASE_ID, "floe.column+proto");
    assertThat(column).isNotEqualTo(relation);
  }

  @Test
  void payloadNormalizationIsCaseInsensitive() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    int lower = generator.generate(BASE_ID, "floe.relation+proto");
    int upper = generator.generate(BASE_ID, "FLOE.RELATION+PROTO");
    assertThat(upper).isEqualTo(lower);
  }

  @Test
  void differentResourceIdsProduceDifferentOids() {
    EngineOidGenerator generator = new DefaultEngineOidGenerator();
    ResourceId other =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("other-table")
            .build();
    int first = generator.generate(BASE_ID, "floe.relation+proto");
    int second = generator.generate(other, "floe.relation+proto");
    assertThat(second).isNotEqualTo(first);
  }
}
