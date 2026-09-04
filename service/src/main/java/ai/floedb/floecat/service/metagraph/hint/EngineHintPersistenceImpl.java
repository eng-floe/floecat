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

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

/** Runtime SPI adapter that persists computed hints without rewriting the relation itself. */
@ApplicationScoped
@Unremovable
public final class EngineHintPersistenceImpl implements EngineHintPersistence {

  private static final Logger LOG = Logger.getLogger(EngineHintPersistenceImpl.class);

  private final TableRepository tables;
  private final ViewRepository views;
  private final HintCache hints;

  @Inject
  public EngineHintPersistenceImpl(TableRepository tables, ViewRepository views, HintCache hints) {
    this.tables = tables;
    this.views = views;
    this.hints = hints;
  }

  @Override
  public void persistRelationHint(
      ResourceId relationId,
      String payloadType,
      String engineKind,
      String engineVersion,
      byte[] payload) {
    persistRelationAndColumnHints(
        relationId, payloadType, payload, engineKind, engineVersion, List.of());
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
      List<EngineHintPersistence.ColumnHint> columnHints) {
    persistRelationAndColumnHints(relationId, null, null, engineKind, engineVersion, columnHints);
  }

  @Override
  public void persistRelationAndColumnHints(
      ResourceId relationId,
      String relationPayloadType,
      byte[] relationPayload,
      String engineKind,
      String engineVersion,
      List<EngineHintPersistence.ColumnHint> columnHints) {
    if (relationId == null
        || (relationPayload == null && (columnHints == null || columnHints.isEmpty()))) {
      return;
    }
    try {
      MutationMeta observed = currentRelation(relationId, false);
      if (observed.getPointerVersion() <= 0L || observed.getBlobUri().isBlank()) {
        return;
      }
      if (hints.containsAll(
          relationId,
          observed.getBlobUri(),
          engineKind,
          engineVersion,
          relationPayloadType,
          relationPayload,
          columnHints)) {
        return;
      }
      MutationMeta relation = currentRelation(relationId, true);
      if (relation.getPointerVersion() <= 0L || relation.getBlobUri().isBlank()) {
        return;
      }
      hints.persist(
          relationId,
          relation,
          engineKind,
          engineVersion,
          relationPayloadType,
          relationPayload,
          columnHints);
    } catch (RuntimeException failure) {
      // Hints are advisory and the runtime deliberately submits this work best-effort. Preserve
      // that contract: a persistence failure must never fail relation decoration.
      LOG.debugf(failure, "Failed to persist engine hints for %s", relationId);
    }
  }

  private MutationMeta currentRelation(ResourceId relationId, boolean consistent) {
    return switch (relationId.getKind()) {
      case RK_TABLE ->
          consistent
              ? tables.pointerMetaForSafeConsistent(relationId)
              : tables.pointerMetaForSafe(relationId);
      case RK_VIEW ->
          consistent
              ? views.pointerMetaForSafeConsistent(relationId)
              : views.pointerMetaForSafe(relationId);
      default -> {
        LOG.debugf("Skipping engine hints for non-relation %s", relationId);
        yield MutationMeta.getDefaultInstance();
      }
    };
  }
}
