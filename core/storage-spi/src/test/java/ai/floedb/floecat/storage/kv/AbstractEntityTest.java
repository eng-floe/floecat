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
package ai.floedb.floecat.storage.kv;

import com.google.protobuf.MessageLite;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public abstract class AbstractEntityTest<M extends MessageLite> {

  protected abstract AbstractEntity<M> getEntity();

  @BeforeEach
  protected void resetTable() {
    getEntity().getKvStore().reset().await().indefinitely();
  }

  @AfterEach
  protected void dumpTable(TestInfo testInfo) {
    getEntity().getKvStore().dump("POST " + testInfo.getDisplayName()).await().indefinitely();
  }

  protected Uni<Map<KvStore.Key, M>> listEntities(String partitionKey, String sortKeyPrefix) {
    return listEntities(getEntity(), partitionKey, sortKeyPrefix);
  }

  protected Uni<Map<KvStore.Key, KvStore.Record>> listRecords(
      String partitionKey, String sortKeyPrefix) {
    return listRecords(getEntity(), partitionKey, sortKeyPrefix);
  }

  public static <M extends MessageLite> Uni<Map<KvStore.Key, M>> listEntities(
      AbstractEntity<M> entity, String partitionKey, String sortKeyPrefix) {
    return listRecords(entity, partitionKey, sortKeyPrefix)
        .onItem()
        .transform(
            records -> {
              var out = new LinkedHashMap<KvStore.Key, M>();
              for (var entry : records.entrySet()) {
                var record = entry.getValue();
                if (record != null && record.kind().equals(entity.getKind())) {
                  var decoded = entity.decode(record);
                  if (decoded != null) {
                    out.put(entry.getKey(), decoded);
                  }
                }
              }
              return out;
            });
  }

  public static <M extends MessageLite> Uni<Map<KvStore.Key, KvStore.Record>> listRecords(
      AbstractEntity<M> entity, String partitionKey, String sortKeyPrefix) {
    var out = new LinkedHashMap<KvStore.Key, KvStore.Record>();
    var tokenRef = new AtomicReference<Optional<String>>(Optional.empty());

    return Multi.createBy()
        .repeating()
        .uni(
            () ->
                entity
                    .getKvStore()
                    .queryByPartitionKeyPrefix(partitionKey, sortKeyPrefix, 100, tokenRef.get()))
        .whilst(page -> !page.nextToken().isEmpty())
        .onItem()
        .invoke(
            page -> {
              for (var r : page.items()) {
                out.put(r.key(), r);
              }
              tokenRef.set(page.nextToken());
            })
        .collect()
        .asList()
        .replaceWith(out);
  }
}
