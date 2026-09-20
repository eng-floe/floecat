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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Common client-side handling for the ordered relation-list result union. */
public final class RelationResults {

  private RelationResults() {}

  /** Reads a page without losing row errors or its continuation token. */
  public static Page read(ListRelationsResponse response) {
    Objects.requireNonNull(response, "response");
    var errors = new ArrayList<RelationListError>();
    var relations = new ArrayList<Relation>();
    response
        .getResultsList()
        .forEach(
            result -> {
              if (result.hasRelation()) {
                relations.add(result.getRelation());
              } else if (result.hasError()) {
                errors.add(result.getError());
              }
            });
    String nextPageToken = response.hasPage() ? response.getPage().getNextPageToken() : "";
    return new Page(relations, errors, nextPageToken);
  }

  /** Formats row errors for adapters that can surface a warning while continuing the listing. */
  public static String describeErrors(List<RelationListError> errors) {
    Objects.requireNonNull(errors, "errors");
    return errors.stream().map(RelationResults::describe).collect(Collectors.joining("; "));
  }

  private static String describe(RelationListError error) {
    String name =
        error.hasName() && !error.getName().getName().isBlank()
            ? error.getName().getName()
            : error.getRelationId().getId();
    String message = error.hasError() ? error.getError().getMessage() : "unknown relation error";
    return name + ": " + message;
  }

  public record Page(
      List<Relation> relations, List<RelationListError> errors, String nextPageToken) {
    public Page {
      relations = List.copyOf(relations);
      errors = List.copyOf(errors);
      nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }
  }
}
