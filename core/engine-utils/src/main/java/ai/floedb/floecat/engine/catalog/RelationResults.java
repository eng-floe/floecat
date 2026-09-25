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

import ai.floedb.floecat.catalog.rpc.ListRelationsResponse;
import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationListError;
import ai.floedb.floecat.catalog.rpc.RelationListResult;
import ai.floedb.floecat.catalog.rpc.ResolveRelationResult;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsResponse;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Common client-side handling for the ordered relation-list result union. */
public final class RelationResults {

  private RelationResults() {}

  /** Reads a page without losing row errors or its continuation token. */
  public static Page read(ListRelationsResponse response) {
    Objects.requireNonNull(response, "response");
    String nextPageToken = response.hasPage() ? response.getPage().getNextPageToken() : "";
    return new Page(response.getResultsList(), nextPageToken);
  }

  /** Formats row errors for adapters that can surface a warning while continuing the listing. */
  public static String describeErrors(List<RelationListError> errors) {
    Objects.requireNonNull(errors, "errors");
    return errors.stream().map(RelationResults::describe).collect(Collectors.joining("; "));
  }

  /**
   * Fails instead of allowing an adapter without partial-result semantics to return a false list.
   */
  public static void requireComplete(Page page) {
    Objects.requireNonNull(page, "page");
    if (!page.errors().isEmpty()) {
      throw new IllegalStateException(
          "relation listing is incomplete: " + describeErrors(page.errors()));
    }
  }

  /**
   * Returns the first resolved relation, preserving the distinction between absence and failure. A
   * missing result is the legacy no-match case; an in-band error is never treated as absence.
   */
  public static Relation requireResolved(ResolveRelationsResponse response) {
    Objects.requireNonNull(response, "response");
    // One result per requested reference, in order. Any other count is the server breaking that
    // contract, which is not the same as the relation being absent and must not read as absence.
    if (response.getResultsCount() != 1) {
      throw new RelationResolutionException(
          Error.newBuilder()
              .setCode(ErrorCode.MC_INTERNAL)
              .setMessage(
                  "resolve response carried "
                      + response.getResultsCount()
                      + " results for one reference")
              .build());
    }

    ResolveRelationResult result = response.getResults(0);
    if (result.hasRelation()) {
      return result.getRelation();
    }
    if (result.hasError()) {
      throw new RelationResolutionException(result.getError());
    }
    throw new RelationResolutionException(
        Error.newBuilder()
            .setCode(ErrorCode.MC_INTERNAL)
            .setMessage("resolve response contained an empty result")
            .build());
  }

  private static String describe(RelationListError error) {
    String name =
        error.hasName() && !error.getName().getName().isBlank()
            ? error.getName().getName()
            : error.getRelationId().getId();
    String message = error.hasError() ? error.getError().getMessage() : "unknown relation error";
    return name + ": " + message;
  }

  public record Page(List<RelationListResult> results, String nextPageToken) {
    public Page {
      results = List.copyOf(results);
      for (int i = 0; i < results.size(); i++) {
        RelationListResult result = results.get(i);
        if (!result.hasRelation() && !result.hasError()) {
          throw new IllegalArgumentException(
              "relation listing row " + i + " has neither a relation nor an error");
        }
      }
      nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }

    public List<Relation> relations() {
      return results.stream()
          .filter(RelationListResult::hasRelation)
          .map(RelationListResult::getRelation)
          .toList();
    }

    public List<RelationListError> errors() {
      return results.stream()
          .filter(RelationListResult::hasError)
          .map(RelationListResult::getError)
          .toList();
    }
  }

  /** Structured failure returned by {@link #requireResolved(ResolveRelationsResponse)}. */
  public static final class RelationResolutionException extends RuntimeException {
    private final Error error;

    private RelationResolutionException(Error error) {
      super(format(error));
      this.error = Objects.requireNonNull(error, "error");
    }

    public Error error() {
      return error;
    }

    public boolean isNotFound() {
      return error.getCode() == ErrorCode.MC_NOT_FOUND;
    }

    private static String format(Error error) {
      String message =
          error.getMessage().isBlank() ? "relation resolution failed" : error.getMessage();
      return error.getCode().name() + ": " + message;
    }
  }
}
