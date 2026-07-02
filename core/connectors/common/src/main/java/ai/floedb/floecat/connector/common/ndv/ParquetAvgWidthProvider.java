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

package ai.floedb.floecat.connector.common.ndv;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

/**
 * Reads per-column average <em>uncompressed</em> width from Parquet footer metadata without reading
 * data pages.
 *
 * <p>For each requested column, sums {@code ColumnChunkMetaData.getTotalUncompressedSize()} across
 * all row groups and divides by the file's total row count. This is a footer-only operation and
 * completes in milliseconds regardless of file size.
 *
 * <p>Uncompressed size is used rather than compressed size because PG's {@code stawidth} represents
 * the average uncompressed byte width, and compressed sizes can integer-divide to 0 for highly
 * compressed columns (e.g. dictionary-encoded enums), making the stat useless. Uncompressed size
 * provides a reasonable proxy until a full value-decoding capture supplies exact decoded widths.
 */
public final class ParquetAvgWidthProvider {

  /** Mutable accumulator passed to {@link #contributeAvgWidth}. */
  public static final class AvgWidthAcc {
    private long totalUncompressedBytes;
    private long totalRowCount;

    public void addBytes(long uncompressedBytes) {
      totalUncompressedBytes += uncompressedBytes;
    }

    public void addRows(long rowCount) {
      totalRowCount += rowCount;
    }

    /**
     * Returns the accumulated average uncompressed bytes per row, or null if no rows were seen.
     *
     * <p>Uses ceiling division (minimum 1) rather than truncating integer division. Parquet-encoded
     * columns (bit-packed integers, dictionary-indexed strings) can have sub-byte-per-row
     * uncompressed sizes; truncating to 0 makes the stat useless since PG filters width <= 0.
     * Ceiling gives at least 1, which is a valid directional signal even for very compact
     * encodings.
     */
    public Long avgWidthBytes() {
      if (totalRowCount == 0) return null;
      // Ceiling division: (a + b - 1) / b ensures minimum 1 for any non-zero bytes.
      return Math.max(1L, (totalUncompressedBytes + totalRowCount - 1) / totalRowCount);
    }

    /** Merge another accumulator into this one (used to fold per-file into cumulative). */
    public void merge(AvgWidthAcc other) {
      totalUncompressedBytes += other.totalUncompressedBytes;
      totalRowCount += other.totalRowCount;
    }
  }

  private final Function<String, org.apache.parquet.io.InputFile> parquetLookup;

  public ParquetAvgWidthProvider(Function<String, org.apache.parquet.io.InputFile> parquetLookup) {
    this.parquetLookup = Objects.requireNonNull(parquetLookup, "parquetLookup");
  }

  public static ParquetAvgWidthProvider forHadoop(org.apache.hadoop.conf.Configuration conf) {
    return new ParquetAvgWidthProvider(
        path -> {
          try {
            return HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), conf);
          } catch (IOException e) {
            throw new RuntimeException("Unable to open parquet via Hadoop: " + path, e);
          }
        });
  }

  public static ParquetAvgWidthProvider forIcebergIO(
      Function<String, org.apache.iceberg.io.InputFile> icebergLookup) {
    return new ParquetAvgWidthProvider(
        path -> new ParquetNdvProvider.ParquetInputFileAdapter(icebergLookup.apply(path)));
  }

  /**
   * Reads footer metadata for {@code filePath} and accumulates per-column <em>uncompressed</em>
   * byte widths into the provided sinks. Columns absent from the file schema are silently skipped.
   *
   * <p>We use {@code getTotalUncompressedSize()} rather than the compressed size because:
   *
   * <ul>
   *   <li>PG's {@code stawidth} represents the average uncompressed byte width of a column value.
   *   <li>Highly-compressed columns (e.g. single-char enums like {@code cd_gender}) have compressed
   *       sizes that integer-divide to 0 per row, making the stat useless.
   *   <li>Uncompressed size gives a reasonable approximation of the actual value bytes (Parquet
   *       plain/dictionary encoding is close to the raw representation).
   * </ul>
   *
   * @param filePath Parquet file path
   * @param sinks map from column name to accumulator; modified in place
   */
  public void contributeAvgWidth(String filePath, Map<String, AvgWidthAcc> sinks) {
    if (sinks == null || sinks.isEmpty()) {
      return;
    }

    try (ParquetFileReader reader = ParquetFileReader.open(parquetLookup.apply(filePath))) {
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        long blockRowCount = block.getRowCount();
        Set<String> columnsCountedInBlock = new HashSet<>();
        for (ColumnChunkMetaData col : block.getColumns()) {
          // Parquet column paths are dot-separated; use the first component as the top-level name.
          // Use indexOf instead of split() to avoid regex allocation per column chunk.
          String dotStr = col.getPath().toDotString();
          int dot = dotStr.indexOf('.');
          String colName = dot == -1 ? dotStr : dotStr.substring(0, dot);
          AvgWidthAcc acc = sinks.get(colName);
          if (acc != null) {
            acc.addBytes(col.getTotalUncompressedSize());
            if (columnsCountedInBlock.add(colName)) {
              acc.addRows(blockRowCount);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("avg_width footer scan failed for " + filePath, e);
    }
  }
}
