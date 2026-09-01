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
  private final boolean hasErrorEnvelope;

  public UnityCatalogException(Failure failure, int statusCode, String message) {
    this(failure, statusCode, null, message, null);
  }

  public UnityCatalogException(Failure failure, int statusCode, String message, Throwable cause) {
    this(failure, statusCode, null, message, cause);
  }

  public UnityCatalogException(
      Failure failure, int statusCode, String errorCode, String message, Throwable cause) {
    this(failure, statusCode, errorCode, errorCode != null, message, cause);
  }

  public UnityCatalogException(
      Failure failure,
      int statusCode,
      String errorCode,
      boolean hasErrorEnvelope,
      String message,
      Throwable cause) {
    super(message, cause);
    this.failure = failure;
    this.statusCode = statusCode;
    this.errorCode = errorCode;
    this.hasErrorEnvelope = hasErrorEnvelope;
  }

  public Failure failure() {
    return failure;
  }

  /**
   * The HTTP status this failure was derived from, or {@code -1} when there was no status.
   *
   * <p>{@code -1} covers three cases: the request never went out ({@code INVALID_REQUEST} raised
   * while applying authentication), it went out and did not complete ({@code TRANSPORT}, {@code
   * INTERRUPTED}), and an {@code INVALID_RESPONSE} rejecting the shape of a body that had already
   * parsed. Only the first two mean the workspace was never reached, so {@code -1} alone is not
   * that signal; for {@code NOT_FOUND} and {@code INVALID_REQUEST}, where a shape rejection cannot
   * arise, it is.
   *
   * <p>A new throw site with no status must pass {@code -1}, not {@code 0}: callers separating the
   * two cases test {@code statusCode() >= 0}.
   */
  public int statusCode() {
    return statusCode;
  }

  /**
   * Whether the response carried Databricks' error envelope at all, which is not the same question
   * as {@link #errorCode()} being non-null.
   *
   * <p>The code is withheld from the accessor when the route suppresses response bodies and the
   * value is not shaped like a real code, because it is read from the body being suppressed and
   * could be a reflected secret. That withholding must not also erase the fact that the workspace
   * answered with a reason: a caller deciding whether a refusal is permanent needs to know an
   * envelope was present even when it cannot be shown the contents.
   */
  public boolean hasErrorEnvelope() {
    return hasErrorEnvelope;
  }

  /**
   * Databricks' machine-readable {@code error_code} from the response body, or null when the
   * catalog did not send one. Kept alongside {@link #failure()} for diagnostics: the classification
   * already folds the codes that change retry behaviour into {@code Failure}.
   *
   * <p>Length-capped at the client boundary, so this is never a large slice of response body even
   * when a server puts one in the field. It is still server-controlled text: a caller rendering it
   * anywhere flattens it first.
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

    /**
     * The calling thread was interrupted while the request was in flight.
     *
     * <p>Separate from {@link #TRANSPORT} because the two call for opposite responses: a transport
     * fault may clear on a retry, while an interrupt is a request to stop. The interrupt flag is
     * restored before this is thrown, so the next send on the same thread fails the same way --
     * retrying it would spend the caller's whole budget on a cancellation and delay the shutdown
     * that was asked for.
     */
    INTERRUPTED,

    /**
     * A status that is transient by definition -- 408, 423, 425. A 409 reaches this classification
     * only when its body names no recognized {@code error_code}; a recognized code may classify the
     * conflict differently. The same request is expected to succeed on a later attempt, so a caller
     * that retries should retry this.
     */
    TRANSIENT,

    /**
     * No classification could be made. A 4xx without Databricks' {@code error_code} envelope lands
     * here: it may be an error page from something in front of the workspace rather than the
     * workspace's own answer, so it is neither known-permanent like {@link #INVALID_REQUEST} nor
     * known-retryable like {@link #TRANSIENT}. A caller should apply whatever default it would use
     * for an unrecognized failure.
     */
    OTHER
  }
}
