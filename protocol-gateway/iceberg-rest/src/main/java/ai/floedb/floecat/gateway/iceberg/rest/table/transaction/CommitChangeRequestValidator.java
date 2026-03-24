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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.util.Map;

@ApplicationScoped
public class CommitChangeRequestValidator {
  @Inject CommitRequestValidationHelper validationHelper;

  public Response validate(TransactionCommitRequest.TableChange change) {
    try {
      validateAndParse(change);
      return null;
    } catch (WebApplicationException e) {
      return e.getResponse();
    }
  }

  public ValidatedTableChange validateAndParse(TransactionCommitRequest.TableChange change) {
    Response identifierError =
        validationHelper.validateTableIdentifier(change == null ? null : change.identifier());
    if (identifierError != null) {
      throw new WebApplicationException(identifierError);
    }
    if (change.requirements() == null) {
      throw new WebApplicationException(
          IcebergErrorResponses.validation("requirements are required"));
    }
    if (change.updates() == null) {
      throw new WebApplicationException(IcebergErrorResponses.validation("updates are required"));
    }
    ParsedCommit parsedCommit =
        ParsedCommit.from(new TableRequests.Commit(change.requirements(), change.updates()));
    Response entryError = validateEntries(parsedCommit);
    if (entryError != null) {
      throw new WebApplicationException(entryError);
    }
    return new ValidatedTableChange(change, parsedCommit);
  }

  private Response validateEntries(ParsedCommit commit) {
    for (Map<String, Object> requirement : commit.requirements()) {
      if (requirement == null) {
        return IcebergErrorResponses.validation("commit requirement entry cannot be null");
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (type == null || type.isBlank()) {
        return IcebergErrorResponses.validation("commit requirement missing type");
      }
      if (!CommitUpdateInspector.isSupportedRequirementType(type)) {
        return IcebergErrorResponses.validation("unsupported commit requirement: " + type);
      }
    }
    String unsupported = validationHelper.unsupportedUpdateAction(commit);
    if (unsupported != null) {
      return IcebergErrorResponses.validation("unsupported commit update action: " + unsupported);
    }
    return null;
  }
}
