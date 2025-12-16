package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.SnapshotRef;
import java.util.List;

/**
 * Optional snapshot metadata cached alongside a {@link UserTableNode}.
 *
 * <p>The info mirrors the structures returned by GetCatalogBundle: the currently effective snapshot
 * plus the resolved pins already stored in the {@code QueryContext}. Keeping it as a dedicated
 * record avoids bloating {@link UserTableNode} with repeated repository calls.
 */
public record ResolvedSnapshotInfo(SnapshotRef currentSnapshot, List<SnapshotRef> pinnedSnapshots) {

  public ResolvedSnapshotInfo {
    pinnedSnapshots = List.copyOf(pinnedSnapshots);
  }
}
