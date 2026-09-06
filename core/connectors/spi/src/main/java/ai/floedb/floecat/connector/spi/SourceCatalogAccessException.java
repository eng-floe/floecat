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

package ai.floedb.floecat.connector.spi;

/**
 * A source catalog permanently refused to vend storage credentials -- a condition that must be
 * classified <em>terminal</em>, not retried.
 *
 * <p>The storage service classifies a vend failure by exception type, not by substring-matching the
 * message: a transient failure whose text merely contains "403" (a gateway page, an IAM-propagation
 * denial, a URL with 403 in it) must stay retryable. Iceberg REST surfaces these as its own {@code
 * NotAuthorizedException}/{@code ForbiddenException}; a connector that speaks a different protocol
 * (Unity Catalog over plain HTTP, for example) has no such typed signal, so it throws this instead.
 * Without it, a hard 401/403 escapes as a generic {@link RuntimeException}, is classified {@code
 * INTERNAL} (retryable), and the reconciler retries a job that can never succeed -- exactly the
 * loop a Databricks workspace missing the {@code EXTERNAL USE SCHEMA} privilege would fall into.
 */
public class SourceCatalogAccessException extends RuntimeException {

  /** Which refusal this is, mapped by the caller to the corresponding terminal gRPC status. */
  public enum Denial {
    /** The caller was not authenticated (e.g. a bad or expired token; HTTP 401). */
    UNAUTHENTICATED,
    /** The caller was authenticated but lacks the privilege to vend (e.g. HTTP 403). */
    PERMISSION_DENIED,
    /**
     * The catalog will not vend for this table for a reason that is neither 401 nor 403 and will
     * not change on retry: Databricks answers the credentials endpoint with HTTP 400 plus an {@code
     * error_code} when a table has external access disabled, and with HTTP 404 for a table id it no
     * longer knows. Not an authorization failure, so it maps to a precondition rather than a denial
     * -- but equally permanent, and equally wrong to retry.
     */
    UNSUPPORTED
  }

  private final Denial denial;

  public SourceCatalogAccessException(Denial denial, String message) {
    super(message);
    this.denial = denial;
  }

  public Denial denial() {
    return denial;
  }
}
