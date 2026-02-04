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

package ai.floedb.floecat.service.metagraph.hint;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.jboss.logging.Logger;

/** Stores and updates encoded engine hints inside table/view metadata. */
@ApplicationScoped
public final class EngineHintPersistenceImpl implements EngineHintPersistence {

  private static final Logger LOG = Logger.getLogger(EngineHintPersistenceImpl.class);

  private final TableRepository tableRepository;
  private final ViewRepository viewRepository;

  @Inject
  public EngineHintPersistenceImpl(TableRepository tableRepository, ViewRepository viewRepository) {
    this.tableRepository = tableRepository;
    this.viewRepository = viewRepository;
  }

  @Override
  public void persistRelationHint(
      ResourceId relationId,
      String payloadType,
      String engineKind,
      String engineVersion,
      byte[] payload) {
    if (relationId == null) {
      return;
    }
    ResourceKind kind = relationId.getKind();
    String encoded = EngineHintMetadata.encodeValue(engineKind, engineVersion, payload);
    String key = EngineHintMetadata.tableHintKey(payloadType);
    if (kind == ResourceKind.RK_TABLE) {
      persistTable(
          relationId,
          builder -> {
            String current = builder.getPropertiesMap().get(key);
            if (encoded.equals(current)) {
              return false;
            }
            builder.putProperties(key, encoded);
            return true;
          });
      return;
    }
    if (kind == ResourceKind.RK_VIEW) {
      persistView(
          relationId,
          builder -> {
            String current = builder.getPropertiesMap().get(key);
            if (encoded.equals(current)) {
              return false;
            }
            builder.putProperties(key, encoded);
            return true;
          });
      return;
    }
    LOG.debugf("Skipping engine hint for non-table relation %s", relationId);
  }

  @Override
  public void persistColumnHint(
      ResourceId relationId,
      long columnId,
      String payloadType,
      String engineKind,
      String engineVersion,
      byte[] payload) {
    persistColumnHints(
        relationId,
        engineKind,
        engineVersion,
        List.of(new EngineHintPersistence.ColumnHint(payloadType, columnId, payload)));
  }

  @Override
  public void persistColumnHints(
      ResourceId relationId,
      String engineKind,
      String engineVersion,
      List<EngineHintPersistence.ColumnHint> hints) {
    if (relationId == null
        || hints == null
        || hints.isEmpty()
        || relationId.getKind() != ResourceKind.RK_TABLE) {
      return;
    }
    persistTable(
        relationId,
        builder -> {
          Map<String, byte[]> deduped = new LinkedHashMap<>();
          for (EngineHintPersistence.ColumnHint hint : hints) {
            String key = EngineHintMetadata.columnHintKey(hint.payloadType(), hint.columnId());
            deduped.put(key, hint.payload());
          }
          boolean changed = false;
          for (Map.Entry<String, byte[]> entry : deduped.entrySet()) {
            String key = entry.getKey();
            byte[] payload = entry.getValue();
            String encoded = EngineHintMetadata.encodeValue(engineKind, engineVersion, payload);
            String current = builder.getPropertiesMap().get(key);
            if (encoded.equals(current)) {
              continue;
            }
            builder.putProperties(key, encoded);
            changed = true;
          }
          return changed;
        });
  }

  private void persistTable(ResourceId tableId, Function<Table.Builder, Boolean> modifier) {
    final int maxAttempts = 2;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        Optional<Table> tableOpt = tableRepository.getById(tableId);
        if (tableOpt.isEmpty()) {
          return;
        }
        Table.Builder builder = tableOpt.get().toBuilder();
        Boolean changed = modifier.apply(builder);
        if (!Boolean.TRUE.equals(changed)) {
          return;
        }
        long version = tableRepository.metaForSafe(tableId).getPointerVersion();
        if (tableRepository.update(builder.build(), version)) {
          return;
        }
        LOG.debugf(
            "Concurrent update prevented persisting engine hint for %s (attempt %d)",
            tableId, attempt);
      } catch (StorageNotFoundException e) {
        LOG.debugf(e, "Failed to persist engine hint for %s", tableId);
        return;
      } catch (RuntimeException e) {
        LOG.debugf(e, "Failed to persist engine hint for %s", tableId);
        return;
      }
    }
  }

  private void persistView(ResourceId viewId, Function<View.Builder, Boolean> modifier) {
    final int maxAttempts = 2;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        Optional<View> viewOpt = viewRepository.getById(viewId);
        if (viewOpt.isEmpty()) {
          return;
        }
        View.Builder builder = viewOpt.get().toBuilder();
        Boolean changed = modifier.apply(builder);
        if (!Boolean.TRUE.equals(changed)) {
          return;
        }
        long version = viewRepository.metaForSafe(viewId).getPointerVersion();
        if (viewRepository.update(builder.build(), version)) {
          return;
        }
        LOG.debugf(
            "Concurrent update prevented persisting engine hint for %s (attempt %d)",
            viewId, attempt);
      } catch (StorageNotFoundException e) {
        LOG.debugf(e, "Failed to persist engine hint for %s", viewId);
        return;
      } catch (RuntimeException e) {
        LOG.debugf(e, "Failed to persist engine hint for %s", viewId);
        return;
      }
    }
  }
}
