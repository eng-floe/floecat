package ai.floedb.metacat.service.query.graph.model;

import ai.floedb.metacat.common.rpc.SnapshotRef;
import java.util.List;

/**
 * Optional snapshot metadata cached alongside a {@link TableNode}.
 *
 * <p>The info mirrors the structures returned by GetCatalogBundle: the currently effective snapshot
 * plus the resolved pins already stored in the {@code QueryContext}. Keeping it as a dedicated
 * record avoids bloating {@link TableNode} with repeated repository calls.
 */
public record ResolvedSnapshotInfo(SnapshotRef currentSnapshot, List<SnapshotRef> pinnedSnapshots) {

  public ResolvedSnapshotInfo {
    pinnedSnapshots = List.copyOf(pinnedSnapshots);
  }
}
