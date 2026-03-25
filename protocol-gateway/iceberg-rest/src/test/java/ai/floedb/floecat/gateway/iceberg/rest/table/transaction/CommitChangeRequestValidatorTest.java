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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CommitChangeRequestValidatorTest {
  private final CommitChangeRequestValidator validator = new CommitChangeRequestValidator();

  CommitChangeRequestValidatorTest() {
    validator.validationHelper = new CommitRequestValidationHelper();
  }

  @Test
  void validateRejectsBlankNamespaceSegments() {
    Response response =
        validator.validate(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db", " "), "orders"), List.of(), List.of()));

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void validateRejectsUnsupportedUpdateActionUsingSharedValidator() {
    Response response =
        validator.validate(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(Map.of("action", "drop"))));

    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void validateRejectsMissingUpdateActionUsingSharedValidator() {
    Response response =
        validator.validate(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(),
                List.of(Map.of("value", "x"))));

    IcebergErrorResponse body = assertInstanceOf(IcebergErrorResponse.class, response.getEntity());
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals("unsupported commit update action: <missing>", body.error().message());
  }

  @Test
  void validateAndParseBuildsTypedParsedCommitOnce() {
    ValidatedTableChange validated =
        validator.validateAndParse(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(List.of("db"), "orders"),
                List.of(Map.of("type", "assert-create")),
                List.of(
                    Map.of(
                        "action",
                        "add-snapshot",
                        "snapshot",
                        Map.of("snapshot-id", 7L, "sequence-number", 3L)))));

    assertEquals("orders", validated.identifier().name());
    assertTrue(validated.parsedCommit().parsed().containsSnapshotUpdates());
    assertEquals(List.of(7L), validated.parsedCommit().parsed().addedSnapshotIds());
  }
}
