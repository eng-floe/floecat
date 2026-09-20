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

package ai.floedb.floecat.engine.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ListRelationsResponse;
import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationListError;
import ai.floedb.floecat.catalog.rpc.RelationListResult;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.NameRef;
import org.junit.jupiter.api.Test;

class RelationResultsTest {

  @Test
  void readReturnsRelationsInWireOrderAndContinuation() {
    Relation first = Relation.newBuilder().setDisplayName("first").build();
    Relation second = Relation.newBuilder().setDisplayName("second").build();
    ListRelationsResponse response =
        ListRelationsResponse.newBuilder()
            .addResults(RelationListResult.newBuilder().setRelation(first))
            .addResults(RelationListResult.newBuilder().setRelation(second))
            .setPage(
                ai.floedb.floecat.common.rpc.PageResponse.newBuilder().setNextPageToken("next"))
            .build();

    var page = RelationResults.read(response);
    assertThat(page.relations()).containsExactly(first, second);
    assertThat(page.errors()).isEmpty();
    assertThat(page.nextPageToken()).isEqualTo("next");
  }

  @Test
  void readPreservesRowErrorsInsteadOfDiscardingThePage() {
    ListRelationsResponse response =
        ListRelationsResponse.newBuilder()
            .addResults(
                RelationListResult.newBuilder()
                    .setError(
                        RelationListError.newBuilder()
                            .setName(NameRef.newBuilder().setName("broken"))
                            .setError(Error.newBuilder().setMessage("unreadable"))))
            .setPage(
                ai.floedb.floecat.common.rpc.PageResponse.newBuilder().setNextPageToken("next"))
            .build();

    var page = RelationResults.read(response);
    assertThat(page.relations()).isEmpty();
    assertThat(page.errors()).hasSize(1);
    assertThat(RelationResults.describeErrors(page.errors())).isEqualTo("broken: unreadable");
    assertThat(page.nextPageToken()).isEqualTo("next");
  }
}
