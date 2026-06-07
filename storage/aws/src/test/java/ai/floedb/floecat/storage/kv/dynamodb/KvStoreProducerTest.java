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
import io.quarkus.arc.properties.IfBuildProperty;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class KvStoreProducerTest {

  @Test
  void produces_KvStore_with_expected_table_name() throws Exception {
    KvStoreProducer producer = new KvStoreProducer();

    KvStore kv = producer.coreKvStore(fakeDdb(), "tbl-one");
    assertTrue(kv instanceof DynamoDbKvStore);
    assertEquals("tbl-one", ((DynamoDbKvStore) kv).getTable());
  }

  @Test
  void startup_bootstrap_uses_configured_table_and_ttl_flag() {
    FakeBootstrap bootstrap = new FakeBootstrap();
    KvStoreProducer producer = new KvStoreProducer();
    producer.ddbBootstrap = bootstrap;
    producer.table = "tbl-two";
    producer.ttlEnabled = false;
    producer.kvMode = "dynamodb";
    producer.autoCreate = true;

    producer.onStart(null);
    assertEquals("tbl-two", bootstrap.lastTable);
    assertFalse(bootstrap.lastTtl);
    assertEquals(1, bootstrap.calls);
  }

  @Test
  void startup_bootstrap_applies_ttl_when_enabled() {
    FakeBootstrap bootstrap = new FakeBootstrap();
    KvStoreProducer producer = new KvStoreProducer();
    producer.ddbBootstrap = bootstrap;
    producer.table = "tbl-three";
    producer.ttlEnabled = true;
    producer.kvMode = "dynamodb";
    producer.autoCreate = true;

    producer.onStart(null);
    assertEquals("tbl-three", bootstrap.lastTable);
    assertTrue(bootstrap.lastTtl);
  }

  @Test
  void startup_bootstrap_skips_when_not_using_dynamodb_auto_create() {
    FakeBootstrap bootstrap = new FakeBootstrap();
    KvStoreProducer producer = new KvStoreProducer();
    producer.ddbBootstrap = bootstrap;
    producer.table = "tbl-four";
    producer.ttlEnabled = true;
    producer.kvMode = "memory";
    producer.autoCreate = false;

    producer.onStart(null);
    assertNull(bootstrap.lastTable);
    assertEquals(0, bootstrap.calls);
  }

  @Test
  void producer_is_only_enabled_for_dynamodb_mode() {
    IfBuildProperty gate = KvStoreProducer.class.getAnnotation(IfBuildProperty.class);
    assertNotNull(gate);
    assertEquals("floecat.kv", gate.name());
    assertEquals("dynamodb", gate.stringValue());
  }

  private static DynamoDbAsyncClient fakeDdb() {
    return (DynamoDbAsyncClient)
        Proxy.newProxyInstance(
            DynamoDbAsyncClient.class.getClassLoader(),
            new Class<?>[] {DynamoDbAsyncClient.class},
            (proxy, method, args) -> null);
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
