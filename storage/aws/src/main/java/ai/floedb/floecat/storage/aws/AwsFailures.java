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

package ai.floedb.floecat.storage.aws;

import ai.floedb.floecat.aws.ClosedAwsClientDetector;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageException;
import ai.floedb.floecat.storage.errors.StorageThrottledException;
import ai.floedb.floecat.storage.errors.StorageUnavailableException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.retry.RetryUtils;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/** The one rule both AWS-backed stores raise a failed AWS client call by. */
public final class AwsFailures {
  private static final List<String> THROTTLED_REASONS =
      List.of("ThrottlingError", "ProvisionedThroughputExceeded");
  private static final List<String> RETRYABLE_ERROR_CODES =
      List.of("RequestTimeout", "RequestTimeoutException", "PriorRequestNotComplete");

  private AwsFailures() {}

  /**
   * The failure to raise for {@code failure} from {@code operation}, unwrapped from its completion
   * wrappers:
   *
   * <ul>
   *   <li>a {@link StorageException}: itself;
   *   <li>an interrupt or an aborted request: {@link CancellationException};
   *   <li>a throttle, or a transaction DynamoDB cancelled for throughput, which applied nothing:
   *       {@link StorageThrottledException};
   *   <li>a 5xx, a request timeout, a call timeout, a clock skew, an I/O client fault or a
   *       shut-down connection pool: {@link StorageAbortRetryableException};
   *   <li>any other AWS failure: {@link StorageUnavailableException}.
   * </ul>
   *
   * <p>Anything not from AWS is returned as it is.
   */
  public static RuntimeException toStorageFailure(String operation, Throwable failure) {
    Throwable unwrapped = ClosedAwsClientDetector.unwrapStageFailure(failure);
    if (unwrapped instanceof StorageException storage) {
      return storage;
    }
    String message = operation + " failed: " + unwrapped.getMessage();
    if (unwrapped instanceof InterruptedException || unwrapped instanceof AbortedException) {
      CancellationException cancelled = new CancellationException(message);
      cancelled.initCause(unwrapped);
      return cancelled;
    }
    if (ClosedAwsClientDetector.isConnectionPoolShutdown(unwrapped)) {
      return new StorageAbortRetryableException(message, unwrapped);
    }
    if (unwrapped instanceof SdkException sdk) {
      if (RetryUtils.isThrottlingException(sdk) || throttledTransaction(sdk)) {
        return new StorageThrottledException(message, sdk);
      }
      return retryable(sdk)
          ? new StorageAbortRetryableException(message, sdk)
          : new StorageUnavailableException(message, sdk);
    }
    if (unwrapped instanceof RuntimeException runtime) {
      return runtime;
    }
    return failure instanceof RuntimeException runtime ? runtime : new CompletionException(failure);
  }

  private static boolean throttledTransaction(SdkException failure) {
    return failure instanceof TransactionCanceledException cancelled
        && cancelled.cancellationReasons().stream()
            .anyMatch(
                reason ->
                    reason != null
                        && reason.code() != null
                        && THROTTLED_REASONS.contains(reason.code()));
  }

  private static boolean retryable(SdkException failure) {
    return RetryUtils.isClockSkewException(failure)
        || (failure instanceof SdkServiceException service
            && (service.statusCode() >= 500 || service.statusCode() == 408))
        || (failure instanceof AwsServiceException aws
            && aws.awsErrorDetails() != null
            && RETRYABLE_ERROR_CODES.contains(aws.awsErrorDetails().errorCode()))
        || failure instanceof RetryableException
        || failure instanceof ApiCallTimeoutException
        || failure instanceof ApiCallAttemptTimeoutException
        || failure.getCause() instanceof IOException;
  }
}
