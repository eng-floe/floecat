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

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import jakarta.ws.rs.core.Response;

public final class StageCommitException extends RuntimeException {
  private final Response.Status status;
  private final String errorType;

  private StageCommitException(Response.Status status, String errorType, String message) {
    super(message);
    this.status = status;
    this.errorType = errorType;
  }

  public static StageCommitException validation(String message) {
    return new StageCommitException(Response.Status.BAD_REQUEST, "ValidationException", message);
  }

  public static StageCommitException notFound(String message) {
    return new StageCommitException(Response.Status.NOT_FOUND, "NoSuchObjectException", message);
  }

  public static StageCommitException conflict(String message) {
    return new StageCommitException(Response.Status.CONFLICT, "CommitFailedException", message);
  }

  public Response toResponse() {
    return Response.status(status)
        .entity(
            new IcebergErrorResponse(
                new IcebergError(getMessage(), errorType, status.getStatusCode())))
        .build();
  }
}
