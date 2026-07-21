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
import java.util.function.Function;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@ApplicationScoped
public class DynamoDbClientManager {

  @Inject AwsClients awsClients;

  private final RefreshingAwsClient<DynamoDbClient> client =
      RefreshingAwsClient.withResourceFactory(
          "DynamoDB", () -> awsClients.newDynamoDbClientResource());

  public DynamoDbClient current() {
    return client.current();
  }

  public <R> R call(Function<DynamoDbClient, R> operation) {
    return client.callUnchecked(operation);
  }

  @PreDestroy
  void close() {
    client.close();
  }
}
