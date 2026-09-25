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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ListRelationsResponse;
import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationListError;
import ai.floedb.floecat.catalog.rpc.RelationListResult;
import ai.floedb.floecat.catalog.rpc.ResolveRelationResult;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsResponse;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import org.junit.jupiter.api.Test;

class RelationResultsTest {

  @Test
  void readPreservesOrderedUnionAndContinuation() {
    Relation first = Relation.newBuilder().setDisplayName("first").build();
    Relation second = Relation.newBuilder().setDisplayName("second").build();
    RelationListResult error =
        RelationListResult.newBuilder()
            .setError(RelationListError.newBuilder().setName(NameRef.newBuilder().setName("bad")))
            .build();
    ListRelationsResponse response =
        ListRelationsResponse.newBuilder()
            .addResults(RelationListResult.newBuilder().setRelation(first))
            .addResults(error)
            .addResults(RelationListResult.newBuilder().setRelation(second))
            .setPage(
                ai.floedb.floecat.common.rpc.PageResponse.newBuilder().setNextPageToken("next"))
            .build();

    var page = RelationResults.read(response);
    assertThat(page.results())
        .containsExactly(response.getResults(0), error, response.getResults(2));
    assertThat(page.relations()).containsExactly(first, second);
    assertThat(page.errors()).containsExactly(error.getError());
    assertThat(page.nextPageToken()).isEqualTo("next");
  }

  @Test
  void requireCompleteRejectsPartialPages() {
    ListRelationsResponse response =
        ListRelationsResponse.newBuilder()
            .addResults(
                RelationListResult.newBuilder()
                    .setError(
                        RelationListError.newBuilder()
                            .setName(NameRef.newBuilder().setName("broken"))))
            .build();

    var page = RelationResults.read(response);
    assertThatThrownBy(() -> RelationResults.requireComplete(page))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("broken");
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

  @Test
  void readRejectsRowsWithoutAResult() {
    ListRelationsResponse response =
        ListRelationsResponse.newBuilder()
            .addResults(RelationListResult.getDefaultInstance())
            .build();

    assertThatThrownBy(() -> RelationResults.read(response))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("row 0")
        .hasMessageContaining("neither a relation nor an error");
  }

  @Test
  void requireResolvedRejectsAnEmptyResponseAsAProtocolViolation() {
    // A reference that resolves to nothing comes back as one result carrying MC_NOT_FOUND. Zero
    // results is the server breaking the one-result-per-reference contract, and reading that as
    // absence would let a caller delete or recreate over a relation it never actually checked.
    assertThatThrownBy(
            () -> RelationResults.requireResolved(ResolveRelationsResponse.getDefaultInstance()))
        .isInstanceOf(RelationResults.RelationResolutionException.class)
        .satisfies(
            failure -> {
              var resolution = (RelationResults.RelationResolutionException) failure;
              assertThat(resolution.isNotFound()).isFalse();
              assertThat(resolution.error().getCode()).isEqualTo(ErrorCode.MC_INTERNAL);
            });
  }

  @Test
  void requireResolvedRejectsMoreResultsThanReferences() {
    ResolveRelationsResponse response =
        ResolveRelationsResponse.newBuilder()
            .addResults(
                ResolveRelationResult.newBuilder().setRelation(Relation.getDefaultInstance()))
            .addResults(
                ResolveRelationResult.newBuilder().setRelation(Relation.getDefaultInstance()))
            .build();

    assertThatThrownBy(() -> RelationResults.requireResolved(response))
        .isInstanceOf(RelationResults.RelationResolutionException.class)
        .satisfies(
            failure ->
                assertThat(
                        ((RelationResults.RelationResolutionException) failure).error().getCode())
                    .isEqualTo(ErrorCode.MC_INTERNAL));
  }

  @Test
  void requireResolvedPreservesInBandFailures() {
    ResolveRelationsResponse response =
        ResolveRelationsResponse.newBuilder()
            .addResults(
                ResolveRelationResult.newBuilder()
                    .setError(
                        Error.newBuilder()
                            .setCode(ErrorCode.MC_PERMISSION_DENIED)
                            .setMessage("access denied"))
                    .build())
            .build();

    assertThatThrownBy(() -> RelationResults.requireResolved(response))
        .isInstanceOf(RelationResults.RelationResolutionException.class)
        .hasMessage("MC_PERMISSION_DENIED: access denied")
        .satisfies(
            failure -> {
              var resolution = (RelationResults.RelationResolutionException) failure;
              assertThat(resolution.isNotFound()).isFalse();
              assertThat(resolution.error().getCode()).isEqualTo(ErrorCode.MC_PERMISSION_DENIED);
            });
  }

  @Test
  void requireResolvedRejectsMalformedResultAsInternal() {
    ResolveRelationsResponse response =
        ResolveRelationsResponse.newBuilder()
            .addResults(ResolveRelationResult.getDefaultInstance())
            .build();

    assertThatThrownBy(() -> RelationResults.requireResolved(response))
        .isInstanceOf(RelationResults.RelationResolutionException.class)
        .hasMessage("MC_INTERNAL: resolve response contained an empty result");
  }
}
