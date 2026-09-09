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
package ai.floedb.floecat.storage.kv.dynamodb;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class DynamoDbTablesBootstrapTest {

  @Test
  void retriesWhileEndpointUnreachable_thenSucceeds() {
    SdkClientException unreachable =
        SdkClientException.create(
            "Unable to execute HTTP request", new ConnectException("Connection refused"));
    DescribeTableResponse active =
        DescribeTableResponse.builder()
            .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
            .build();
    AtomicInteger calls = new AtomicInteger();
    DynamoDbTablesBootstrap bootstrap =
        bootstrap(10, describeTableStub(calls, unreachable, unreachable, active));

    bootstrap.ensureTableExists("tbl", false);

    assertEquals(3, calls.get());
  }

  @Test
  void givesUpAfterConnectWait() {
    SdkClientException unreachable =
        SdkClientException.create(
            "Unable to execute HTTP request", new ConnectException("Connection refused"));
    AtomicInteger calls = new AtomicInteger();
    DynamoDbTablesBootstrap bootstrap = bootstrap(0, describeTableStub(calls, unreachable));

    Throwable thrown =
        assertThrows(Throwable.class, () -> bootstrap.ensureTableExists("tbl", false));

    assertTrue(containsCause(thrown, SdkClientException.class));
    assertEquals(1, calls.get());
  }

  @Test
  void serviceErrorIsNotRetried() {
    AwsServiceException notAuthorized =
        DynamoDbException.builder().message("not authorized").build();
    AtomicInteger calls = new AtomicInteger();
    DynamoDbTablesBootstrap bootstrap = bootstrap(10, describeTableStub(calls, notAuthorized));

    assertThrows(Throwable.class, () -> bootstrap.ensureTableExists("tbl", false));

    assertEquals(1, calls.get());
  }

  private static DynamoDbTablesBootstrap bootstrap(
      int connectWaitSeconds, DynamoDbAsyncClient ddb) {
    DynamoDbTablesBootstrap bootstrap = new DynamoDbTablesBootstrap();
    bootstrap.enabled = true;
    bootstrap.waitSeconds = 1;
    bootstrap.connectWaitSeconds = connectWaitSeconds;
    bootstrap.ddb = ddb;
    return bootstrap;
  }

  private static DynamoDbAsyncClient describeTableStub(
      AtomicInteger calls, Object... responsesOrExceptions) {
    List<Object> results = List.of(responsesOrExceptions);
    return (DynamoDbAsyncClient)
        Proxy.newProxyInstance(
            DynamoDbAsyncClient.class.getClassLoader(),
            new Class<?>[] {DynamoDbAsyncClient.class},
            (proxy, method, args) -> {
              if (!"describeTable".equals(method.getName())) {
                return null;
              }
              int index = calls.getAndIncrement();
              Object result = results.get(Math.min(index, results.size() - 1));
              if (result instanceof Throwable t) {
                return CompletableFuture.failedFuture(t);
              }
              return CompletableFuture.completedFuture(result);
            });
  }

  private static boolean containsCause(Throwable t, Class<? extends Throwable> type) {
    Throwable cur = t;
    while (cur != null) {
      if (type.isInstance(cur)) return true;
      cur = cur.getCause();
    }
    return false;
  }
}
