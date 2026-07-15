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

package ai.floedb.floecat.service.query;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_BLOB_VERSION_MISMATCH;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_ROOT_MISSING;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_SNAPSHOT_BLOB_MISSING;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_PINNED_TABLE_BLOB_MISSING;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

/**
 * Validation that a {@link TablePin}'s ROOT — the one immutable object every read follows refs out
 * of — is present at the exact version captured when the pin was created: one HEAD on the
 * content-addressed root blob. The refs the pin copied (definition, snapshot, constraints) came out
 * of that root, are GC-rooted transitively while the pin lives, and every read that loads one fails
 * loudly through {@link #requirePinnedTableBlob}/{@link #requirePinnedSnapshotBlob} if it is gone —
 * so the single root leg is the pin's integrity contract, not a per-blob re-check.
 *
 * <p>A pin with an empty root URI was never legitimately constructed (construction reads the root
 * or fails) and is rejected loudly rather than waved through.
 */
@ApplicationScoped
public class PinValidator {

  private final TableRootRepository roots;

  @Inject
  public PinValidator(TableRootRepository roots) {
    this.roots = roots;
  }

  /**
   * Unwrap a pinned-table-blob load, failing with the same catalog-integrity error every pinned
   * read uses when the blob is gone. Keeps the error contract for "the pinned blob vanished" in one
   * place across the schema/scan read sites.
   */
  public static <T> T requirePinnedTableBlob(
      java.util.Optional<T> loaded, String correlationId, ResourceId tableId) {
    return loaded.orElseThrow(
        () ->
            GrpcErrors.internal(
                correlationId,
                QUERY_PINNED_TABLE_BLOB_MISSING,
                Map.of("table_id", tableId.getId())));
  }

  /** Snapshot-blob variant of {@link #requirePinnedTableBlob} for sites without the snapshot id. */
  public static <T> T requirePinnedSnapshotBlob(
      java.util.Optional<T> loaded, String correlationId, ResourceId tableId) {
    return loaded.orElseThrow(
        () ->
            GrpcErrors.internal(
                correlationId,
                QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
                Map.of("table_id", tableId.getId())));
  }

  /** Snapshot-blob variant carrying the snapshot id in the error payload. */
  public static <T> T requirePinnedSnapshotBlob(
      java.util.Optional<T> loaded, String correlationId, ResourceId tableId, long snapshotId) {
    return loaded.orElseThrow(
        () ->
            GrpcErrors.internal(
                correlationId,
                QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
                Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId))));
  }

  /** Validate the pin's root leg, throwing a catalog-integrity error when it does not hold. */
  public void validate(String correlationId, TablePin pin) {
    if (pin.getRootUri().isEmpty()) {
      throw GrpcErrors.internal(
          correlationId, QUERY_PINNED_ROOT_MISSING, Map.of("table_id", pin.getTableId().getId()));
    }
    String etag = roots.blobEtag(pin.getRootUri());
    if (etag == null) {
      throw GrpcErrors.internal(
          correlationId, QUERY_PINNED_ROOT_MISSING, Map.of("table_id", pin.getTableId().getId()));
    }
    if (!pin.getRootVersion().isEmpty() && !etag.equals(pin.getRootVersion())) {
      throw GrpcErrors.internal(
          correlationId,
          QUERY_PINNED_BLOB_VERSION_MISMATCH,
          Map.of(
              "table_id",
              pin.getTableId().getId(),
              "kind",
              "root",
              "pinned_version",
              pin.getRootVersion(),
              "found_version",
              etag));
    }
  }
}
