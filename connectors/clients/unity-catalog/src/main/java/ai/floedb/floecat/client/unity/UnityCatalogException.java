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

package ai.floedb.floecat.client.unity;

/** A transport-neutral failure from Unity Catalog. */
public final class UnityCatalogException extends RuntimeException {
  private final Failure failure;
  private final int statusCode;
  private final String errorCode;

  public UnityCatalogException(Failure failure, int statusCode, String message) {
    this(failure, statusCode, null, message, null);
  }

  public UnityCatalogException(Failure failure, int statusCode, String message, Throwable cause) {
    this(failure, statusCode, null, message, cause);
  }

  public UnityCatalogException(
      Failure failure, int statusCode, String errorCode, String message, Throwable cause) {
    super(message, cause);
    this.failure = failure;
    this.statusCode = statusCode;
    this.errorCode = errorCode;
  }

  public Failure failure() {
    return failure;
  }

  public int statusCode() {
    return statusCode;
  }

  /**
   * Databricks' machine-readable {@code error_code} from the response body, or null when the
   * catalog did not send one. Kept alongside {@link #failure()} for diagnostics: the classification
   * already folds the codes that change retry behaviour into {@code Failure}.
   */
  public String errorCode() {
    return errorCode;
  }

  public enum Failure {
    UNAUTHENTICATED,
    PERMISSION_DENIED,
    NOT_FOUND,
    RATE_LIMITED,
    SERVER_ERROR,
    TRANSPORT,
    INVALID_RESPONSE,

    /**
     * A 4xx the catalog will keep returning for this request -- a Databricks {@code 400
     * INVALID_PARAMETER_VALUE} for a table without external access, a 405, a 422. Separate from
     * {@link #OTHER} because it is permanent: retrying it can only loop.
     */
    INVALID_REQUEST,
    OTHER
  }
}
