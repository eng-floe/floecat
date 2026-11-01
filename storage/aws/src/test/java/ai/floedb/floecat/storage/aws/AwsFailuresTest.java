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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageThrottledException;
import ai.floedb.floecat.storage.errors.StorageUnavailableException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

class AwsFailuresTest {

  @Test
  void raisesEachAwsFailureAsTheStorageFailureItIs() {
    Map<Throwable, Class<?>> expected = new LinkedHashMap<>();
    expected.put(dynamo(400, "ThrottlingException"), StorageThrottledException.class);
    expected.put(dynamo(500, "InternalServerError"), StorageAbortRetryableException.class);
    expected.put(dynamo(400, "ValidationException"), StorageUnavailableException.class);
    expected.put(dynamo(400, "RequestTimeoutException"), StorageAbortRetryableException.class);
    expected.put(
        SdkClientException.builder().message("reset").cause(new IOException("reset")).build(),
        StorageAbortRetryableException.class);
    expected.put(RetryableException.create("read timed out"), StorageAbortRetryableException.class);
    expected.put(
        ApiCallAttemptTimeoutException.builder().message("attempt timed out").build(),
        StorageAbortRetryableException.class);
    expected.put(
        SdkClientException.builder().message("unable to load credentials").build(),
        StorageUnavailableException.class);
    expected.put(
        new CompletionException(new RuntimeException("Connection pool shut down")),
        StorageAbortRetryableException.class);
    expected.put(closedClient(), RefreshingAwsClient.ClosedException.class);
    expected.put(new CompletionException(new InterruptedException()), CancellationException.class);
    expected.put(new IllegalArgumentException("bad key"), IllegalArgumentException.class);

    expected.forEach(
        (failure, type) ->
            assertThat(AwsFailures.toStorageFailure("op", failure))
                .as(failure.toString())
                .isExactlyInstanceOf(type));
  }

  private static DynamoDbException dynamo(int status, String errorCode) {
    return (DynamoDbException)
        DynamoDbException.builder()
            .statusCode(status)
            .message(errorCode)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
            .build();
  }

  private static RuntimeException closedClient() {
    RefreshingAwsClient<AutoCloseable> client = new RefreshingAwsClient<>(() -> () -> {});
    client.close();
    try {
      client.current();
    } catch (RefreshingAwsClient.ClosedException closed) {
      return closed;
    }
    throw new AssertionError("a closed client must refuse calls");
  }
}
