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

import ai.floedb.floecat.storage.kv.KvStore;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class KvStoreProducerTest {

  @Test
  void produces_KvStore_with_expected_table_name() throws Exception {
    FakeBootstrap bootstrap = new FakeBootstrap();
    KvStoreProducer producer = producerWith(bootstrap);

    KvStore kv = producer.coreKvStore(fakeDdb(), "tbl-one", true);
    assertTrue(kv instanceof DynamoDbKvStore);
    assertEquals("tbl-one", tableName((DynamoDbKvStore) kv));
  }

  @Test
  void calls_bootstrap_ensureTableExists_with_ttl_flag() {
    FakeBootstrap bootstrap = new FakeBootstrap();
    KvStoreProducer producer = producerWith(bootstrap);

    producer.coreKvStore(fakeDdb(), "tbl-two", false);
    assertEquals("tbl-two", bootstrap.lastTable);
    assertFalse(bootstrap.lastTtl);
    assertEquals(1, bootstrap.calls);
  }

  @Test
  void ttl_enabled_default_true_is_applied() {
    FakeBootstrap bootstrap = new FakeBootstrap();
    KvStoreProducer producer = producerWith(bootstrap);

    producer.coreKvStore(fakeDdb(), "tbl-three", true);
    assertEquals("tbl-three", bootstrap.lastTable);
    assertTrue(bootstrap.lastTtl);
  }

  private static KvStoreProducer producerWith(FakeBootstrap bootstrap) {
    KvStoreProducer producer = new KvStoreProducer();
    producer.ddbBootstrap = bootstrap;
    return producer;
  }

  private static DynamoDbAsyncClient fakeDdb() {
    return (DynamoDbAsyncClient)
        Proxy.newProxyInstance(
            DynamoDbAsyncClient.class.getClassLoader(),
            new Class<?>[] {DynamoDbAsyncClient.class},
            (proxy, method, args) -> null);
  }

  private static String tableName(DynamoDbKvStore store) throws Exception {
    Field field = DynamoDbKvStore.class.getDeclaredField("table");
    field.setAccessible(true);
    return (String) field.get(store);
  }

  private static final class FakeBootstrap extends DynamoDbTablesBootstrap {
    private String lastTable;
    private boolean lastTtl;
    private int calls;

    @Override
    public void ensureTableExists(String tableName, boolean withTtl) {
      this.lastTable = tableName;
      this.lastTtl = withTtl;
      this.calls++;
    }
  }
}
