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

  public UnityCatalogException(Failure failure, int statusCode, String message) {
    super(message);
    this.failure = failure;
    this.statusCode = statusCode;
  }

  public UnityCatalogException(Failure failure, int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.failure = failure;
    this.statusCode = statusCode;
  }

  public Failure failure() {
    return failure;
  }

  public int statusCode() {
    return statusCode;
  }

  public enum Failure {
    UNAUTHENTICATED,
    PERMISSION_DENIED,
    NOT_FOUND,
    RATE_LIMITED,
    SERVER_ERROR,
    TRANSPORT,
    INVALID_RESPONSE,
    OTHER
  }
}
