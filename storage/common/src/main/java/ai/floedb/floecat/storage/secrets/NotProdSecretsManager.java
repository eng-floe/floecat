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

package ai.floedb.floecat.storage.secrets;

import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.cdi.KvTable;
import io.quarkus.arc.profile.UnlessBuildProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
@UnlessBuildProfile("prod")
public class NotProdSecretsManager implements SecretsManager {
  private static final String KIND = "Secret";
  private static final String PARTITION_KEY = "secrets";

  @Inject
  @KvTable("floecat")
  KvStore kv;

  @Override
  public void put(String accountId, String secretType, String secretId, byte[] payload) {
    KvStore.Key key = keyFor(accountId, secretType, secretId);
    KvStore.Record record = new KvStore.Record(key, KIND, copy(payload), Map.of(), 1L);
    boolean ok = kv.putCas(record, 0L).await().indefinitely();
    if (!ok) {
      throw new IllegalStateException("Secret already exists: " + key.sortKey());
    }
  }

  @Override
  public Optional<byte[]> get(String accountId, String secretType, String secretId) {
    KvStore.Key key = keyFor(accountId, secretType, secretId);
    return kv.get(key).await().indefinitely().map(rec -> copy(rec.value()));
  }

  @Override
  public void update(String accountId, String secretType, String secretId, byte[] payload) {
    KvStore.Key key = keyFor(accountId, secretType, secretId);
    Optional<KvStore.Record> existing = kv.get(key).await().indefinitely();
    long expectedVersion = existing.map(KvStore.Record::version).orElse(0L);
    long nextVersion = expectedVersion == 0L ? 1L : expectedVersion + 1L;
    KvStore.Record record = new KvStore.Record(key, KIND, copy(payload), Map.of(), nextVersion);
    boolean ok = kv.putCas(record, expectedVersion).await().indefinitely();
    if (!ok) {
      throw new IllegalStateException("Secret update conflict: " + key.sortKey());
    }
  }

  @Override
  public void delete(String accountId, String secretType, String secretId) {
    KvStore.Key key = keyFor(accountId, secretType, secretId);
    Optional<KvStore.Record> existing = kv.get(key).await().indefinitely();
    if (existing.isEmpty()) {
      return;
    }
    boolean ok = kv.deleteCas(key, existing.get().version()).await().indefinitely();
    if (!ok) {
      throw new IllegalStateException("Secret delete conflict: " + key.sortKey());
    }
  }

  private static KvStore.Key keyFor(String accountId, String secretType, String secretId) {
    String key = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    return new KvStore.Key(PARTITION_KEY, key);
  }

  private static byte[] copy(byte[] payload) {
    return payload == null ? null : payload.clone();
  }
}
