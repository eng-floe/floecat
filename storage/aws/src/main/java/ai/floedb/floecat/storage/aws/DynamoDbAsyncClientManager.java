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

import ai.floedb.floecat.aws.RefreshingAwsClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@ApplicationScoped
public class DynamoDbAsyncClientManager {

  @Inject AwsClients awsClients;

  private final RefreshingAwsClient<DynamoDbAsyncClient> client =
      RefreshingAwsClient.withResourceFactory(
          "DynamoDB async", () -> awsClients.newDynamoDbAsyncClientResource());

  public DynamoDbAsyncClient current() {
    return client.current();
  }

  public <R> R call(Function<DynamoDbAsyncClient, R> operation) {
    return client.callUnchecked(operation);
  }

  public <R> CompletableFuture<R> callAsync(
      Function<DynamoDbAsyncClient, ? extends CompletionStage<R>> operation) {
    return client.callAsync(operation);
  }

  @PreDestroy
  void close() {
    client.close();
  }
}
