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
package ai.floedb.floecat.client.sharing;

/**
 * A Delta Sharing request that did not produce a usable answer, classified by what a caller can do
 * about it.
 *
 * <p>The classification is the point of this type. A recipient talks to a server it does not
 * administer, so the difference between "this token is not accepted", "this share is not shared
 * with you", "come back later" and "the server sent something this client cannot read" decides
 * whether a reconcile job retries, fails, or reports a configuration error to an operator.
 */
public final class DeltaSharingException extends RuntimeException {

  /** What kind of failure occurred, from the caller's point of view. */
  public enum Failure {
    /** The recipient token was rejected. */
    UNAUTHENTICATED,
    /** The token is valid but does not carry access to the requested share, schema or table. */
    PERMISSION_DENIED,
    /** The share, schema or table does not exist, or is not shared with this recipient. */
    NOT_FOUND,
    /** The server asked the client to slow down. */
    RATE_LIMITED,
    /** The server failed. Retrying may succeed. */
    SERVER_ERROR,
    /** The request never reached the server, or its response never arrived. */
    TRANSPORT,
    /** A response arrived that this client cannot parse or that violates the protocol. */
    INVALID_RESPONSE,
    /** The server rejected the request as malformed or unsupported. */
    INVALID_REQUEST,
    /** The calling thread was interrupted. Not a server condition and not worth a retry budget. */
    INTERRUPTED,
    /** Anything else the server reported that a later attempt could clear. */
    TRANSIENT,
    /** Unclassified. */
    OTHER
  }

  private final Failure failure;
  private final int statusCode;

  public DeltaSharingException(Failure failure, int statusCode, String message) {
    this(failure, statusCode, message, null);
  }

  public DeltaSharingException(Failure failure, int statusCode, String message, Throwable cause) {
    super(message, cause);
    this.failure = failure == null ? Failure.OTHER : failure;
    this.statusCode = statusCode;
  }

  public Failure failure() {
    return failure;
  }

  /** The HTTP status that produced this, or -1 when the failure happened before a response. */
  public int statusCode() {
    return statusCode;
  }
}
