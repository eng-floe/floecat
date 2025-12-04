package ai.floedb.metacat.service.query.graph.model;

import java.util.Map;

/**
 * Lightweight statistics summary cached with {@link TableNode}.
 *
 * <p>The full statistics payloads (table/column/file stats) can be large, so this record captures
 * only a bounded subset (row count, data size, NDV sketches) that planners commonly use for
 * pruning. Consumers can always fall back to the statistics RPCs if they need detailed histograms.
 */
public record TableStatsSummary(
    long rowCount, long dataSizeBytes, Map<String, Double> ndvPerColumn) {

  public TableStatsSummary {
    ndvPerColumn = Map.copyOf(ndvPerColumn);
  }
}
