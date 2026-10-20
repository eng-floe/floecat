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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.NodeKind;
import io.delta.kernel.Snapshot;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Resolved source facts needed for identity reconciliation, independent of scan operations. */
public record DeltaIdentitySnapshot(
    long version, DeltaResolvedSchema resolved, long nativeHighWaterMark) {
  public DeltaIdentitySnapshot {
    if (version < 0L) {
      throw new IllegalArgumentException("Delta identity version must be non-negative");
    }
    Objects.requireNonNull(resolved, "resolved");
    if (nativeHighWaterMark < 0L) {
      throw new IllegalArgumentException("Native high-water mark must be non-negative");
    }
  }

  public static DeltaIdentitySnapshot from(Snapshot snapshot) {
    DeltaResolvedSchema resolved = DeltaColumnMapping.resolveSchema(snapshot);
    return new DeltaIdentitySnapshot(
        snapshot.getVersion(),
        resolved,
        resolved.effectiveMappingMode().isEnabled()
            ? DeltaColumnMapping.maxColumnId(snapshot)
            : 0L);
  }

  public IdentityMode mode() {
    return DeltaCanonicalIdentity.identityMode(resolved);
  }

  public Set<ColumnPath> fieldPaths() {
    Set<ColumnPath> paths = new LinkedHashSet<>();
    resolved.schema().nodes().stream()
        .filter(node -> node.kind() == NodeKind.FIELD)
        .forEach(node -> paths.add(node.path()));
    return Set.copyOf(paths);
  }

  public Set<Integer> nativeIds() {
    Set<Integer> ids = new LinkedHashSet<>();
    resolved.schema().nodes().stream()
        .filter(node -> node.kind() == NodeKind.FIELD)
        .forEach(node -> node.nativeFieldId().ifPresent(ids::add));
    return Set.copyOf(ids);
  }
}
