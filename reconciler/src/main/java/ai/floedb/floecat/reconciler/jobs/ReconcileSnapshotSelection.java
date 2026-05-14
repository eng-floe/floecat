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

package ai.floedb.floecat.reconciler.jobs;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/** Explicit upstream snapshot selection for reconcile planning. */
public record ReconcileSnapshotSelection(Kind kind, List<Long> snapshotIds, int latestN) {
  public enum Kind {
    UNSPECIFIED,
    CURRENT,
    LATEST_N,
    EXPLICIT,
    ALL
  }

  private static final ReconcileSnapshotSelection EMPTY =
      new ReconcileSnapshotSelection(Kind.UNSPECIFIED, List.of(), 0);
  private static final ReconcileSnapshotSelection CURRENT =
      new ReconcileSnapshotSelection(Kind.CURRENT, List.of(), 0);
  private static final ReconcileSnapshotSelection ALL =
      new ReconcileSnapshotSelection(Kind.ALL, List.of(), 0);

  public ReconcileSnapshotSelection {
    kind = kind == null ? Kind.UNSPECIFIED : kind;
    snapshotIds =
        snapshotIds == null
            ? List.of()
            : snapshotIds.stream()
                .filter(Objects::nonNull)
                .map(Long::longValue)
                .filter(id -> id >= 0L)
                .collect(
                    java.util.stream.Collectors.collectingAndThen(
                        java.util.stream.Collectors.toCollection(LinkedHashSet::new),
                        List::copyOf));
    latestN = Math.max(0, latestN);
    switch (kind) {
      case CURRENT, ALL, UNSPECIFIED -> {
        if (!snapshotIds.isEmpty() || latestN != 0) {
          throw new IllegalArgumentException(kind + " snapshot selection cannot carry ids/count");
        }
      }
      case LATEST_N -> {
        if (latestN <= 0 || !snapshotIds.isEmpty()) {
          throw new IllegalArgumentException("LATEST_N requires latestN > 0 and no explicit ids");
        }
      }
      case EXPLICIT -> {
        if (snapshotIds.isEmpty() || latestN != 0) {
          throw new IllegalArgumentException(
              "EXPLICIT requires snapshotIds and cannot carry latestN");
        }
      }
    }
  }

  public static ReconcileSnapshotSelection unspecified() {
    return EMPTY;
  }

  public static ReconcileSnapshotSelection current() {
    return CURRENT;
  }

  public static ReconcileSnapshotSelection latestN(int latestN) {
    return new ReconcileSnapshotSelection(Kind.LATEST_N, List.of(), latestN);
  }

  public static ReconcileSnapshotSelection explicit(List<Long> snapshotIds) {
    return new ReconcileSnapshotSelection(Kind.EXPLICIT, snapshotIds, 0);
  }

  public static ReconcileSnapshotSelection all() {
    return ALL;
  }

  public boolean isSpecified() {
    return kind != Kind.UNSPECIFIED;
  }
}
