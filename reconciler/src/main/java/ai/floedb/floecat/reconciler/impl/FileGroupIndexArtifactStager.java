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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexCoverage;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import com.google.protobuf.Timestamp;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public final class FileGroupIndexArtifactStager {
  public static final String INDEX_CONTENT_TYPE = "application/x-parquet";

  private static final MessageType BOOTSTRAP_INDEX_SIDECAR_SCHEMA =
      MessageTypeParser.parseMessageType(
          """
          message arrow_schema {
            required binary column_name (STRING);
            required int32 row_group (INTEGER(32,false));
            required int32 page_ordinal (INTEGER(32,false));
            required int64 first_row_index;
            required int32 row_count (INTEGER(32,false));
            required int32 live_row_count (INTEGER(32,false));
            optional int64 page_header_offset;
            required int32 page_total_compressed_size;
            optional int64 dictionary_page_header_offset;
            optional int32 dictionary_page_total_compressed_size;
            required boolean requires_dictionary_page;
            required binary parquet_physical_type (STRING);
            required binary parquet_compression (STRING);
            required int32 parquet_max_def_level (INTEGER(16,true));
            required int32 parquet_max_rep_level (INTEGER(16,true));
            optional int32 min_i32;
            optional int32 max_i32;
            optional int64 min_i64;
            optional int64 max_i64;
            optional float min_f32;
            optional float max_f32;
            optional double min_f64;
            optional double max_f64;
            optional boolean min_bool;
            optional boolean max_bool;
            optional binary min_utf8 (STRING);
            optional binary max_utf8 (STRING);
            optional int32 decimal_precision (INTEGER(8,false));
            optional int32 decimal_scale (INTEGER(8,true));
            optional int32 decimal_bits (INTEGER(16,true));
            optional fixed_len_byte_array(16) min_decimal128_unscaled (DECIMAL(38,0));
            optional fixed_len_byte_array(16) max_decimal128_unscaled (DECIMAL(38,0));
            optional fixed_len_byte_array(32) min_decimal256_unscaled (DECIMAL(76,0));
            optional fixed_len_byte_array(32) max_decimal256_unscaled (DECIMAL(76,0));
          }
          """);

  private FileGroupIndexArtifactStager() {}

  public static List<ReconcilerBackend.StagedIndexArtifact> stage(
      ResourceId tableId,
      long snapshotId,
      List<String> plannedFilePaths,
      List<TargetStatsRecord> stats,
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    if (plannedFilePaths == null || plannedFilePaths.isEmpty()) {
      return List.of();
    }
    Map<String, List<FloecatConnector.ParquetPageIndexEntry>> pageEntriesByFile =
        pageIndexEntriesByFile(pageIndexEntries);
    Map<String, FileTargetStats> byPath = fileStatsByPath(stats);
    if (byPath.isEmpty() && pageEntriesByFile.isEmpty()) {
      return List.of();
    }
    var now = nowTs();
    List<ReconcilerBackend.StagedIndexArtifact> artifacts = new ArrayList<>();
    for (String filePath : plannedFilePaths) {
      List<FloecatConnector.ParquetPageIndexEntry> filePageEntries =
          pageEntriesByFile.getOrDefault(filePath, List.of());
      FileTargetStats fileStats = byPath.get(filePath);
      if (fileStats == null && !filePageEntries.isEmpty()) {
        fileStats = synthesizeIndexOnlyFileStats(filePath, filePageEntries);
      }
      if (fileStats == null) {
        continue;
      }
      byte[] sidecar = writeIndexSidecar(fileStats, filePageEntries);
      String contentSha256B64 = sha256B64(sidecar);
      IndexArtifactRecord.Builder record =
          IndexArtifactRecord.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setTarget(
                  IndexTarget.newBuilder()
                      .setFile(IndexFileTarget.newBuilder().setFilePath(filePath).build())
                      .build())
              .setArtifactUri(
                  snapshotIndexSidecarBlobUri(
                      tableId.getAccountId(),
                      tableId.getId(),
                      snapshotId,
                      indexArtifactTargetStorageId(filePath),
                      base64ToHex(contentSha256B64)))
              .setArtifactFormat("parquet")
              .setArtifactFormatVersion(1)
              .setContentEtag(contentSha256B64)
              .setContentSha256B64(contentSha256B64)
              .setState(IndexArtifactState.IAS_READY)
              .setCoverage(indexCoverage(fileStats, filePageEntries))
              .setCreatedAt(now)
              .setRefreshedAt(now)
              .setSourceFileFormat(
                  fileStats.getFileFormat().isBlank() ? "parquet" : fileStats.getFileFormat())
              .putProperties(
                  "materialization", filePageEntries.isEmpty() ? "bootstrap" : "page_index");
      if (fileStats.hasSequenceNumber()) {
        record.putProperties(
            "source_sequence_number", Long.toString(fileStats.getSequenceNumber()));
      }
      if (fileStats.getFileContentValue() != 0) {
        record.putProperties("source_file_content", fileStats.getFileContent().name());
      }
      String indexedColumns = indexedColumnsProperty(filePageEntries);
      if (!indexedColumns.isBlank()) {
        record.putProperties("indexed_columns", indexedColumns);
      }
      artifacts.add(
          new ReconcilerBackend.StagedIndexArtifact(record.build(), sidecar, INDEX_CONTENT_TYPE));
    }
    return List.copyOf(artifacts);
  }

  private static Timestamp nowTs() {
    long nowMs = System.currentTimeMillis();
    return Timestamp.newBuilder()
        .setSeconds(nowMs / 1000L)
        .setNanos((int) ((nowMs % 1000L) * 1_000_000L))
        .build();
  }

  private static Map<String, FileTargetStats> fileStatsByPath(List<TargetStatsRecord> stats) {
    HashMap<String, FileTargetStats> byPath = new HashMap<>();
    if (stats == null) {
      return byPath;
    }
    for (TargetStatsRecord record : stats) {
      if (!record.hasFile()) {
        continue;
      }
      FileTargetStats fileStats = record.getFile();
      if (fileStats.getFilePath() == null || fileStats.getFilePath().isBlank()) {
        continue;
      }
      byPath.put(fileStats.getFilePath(), fileStats);
    }
    return byPath;
  }

  private static Map<String, List<FloecatConnector.ParquetPageIndexEntry>> pageIndexEntriesByFile(
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    HashMap<String, List<FloecatConnector.ParquetPageIndexEntry>> byPath = new HashMap<>();
    if (pageIndexEntries == null || pageIndexEntries.isEmpty()) {
      return Map.of();
    }
    for (FloecatConnector.ParquetPageIndexEntry entry : pageIndexEntries) {
      if (entry == null || entry.filePath() == null || entry.filePath().isBlank()) {
        continue;
      }
      byPath.computeIfAbsent(entry.filePath(), ignored -> new ArrayList<>()).add(entry);
    }
    return byPath;
  }

  private static FileTargetStats synthesizeIndexOnlyFileStats(
      String filePath, List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    FileTargetStats.Builder builder =
        FileTargetStats.newBuilder().setFilePath(filePath).setFileFormat("parquet");
    long rowsIndexed =
        pageIndexEntries == null
            ? 0L
            : pageIndexEntries.stream()
                .map(FloecatConnector.ParquetPageIndexEntry::liveRowCount)
                .filter(value -> value > 0)
                .mapToLong(Integer::longValue)
                .max()
                .orElse(0L);
    if (rowsIndexed > 0L) {
      builder.setRowCount(rowsIndexed);
    }
    long bytesScanned =
        pageIndexEntries == null
            ? 0L
            : pageIndexEntries.stream()
                .map(FloecatConnector.ParquetPageIndexEntry::pageTotalCompressedSize)
                .filter(value -> value > 0L)
                .mapToLong(Integer::longValue)
                .sum();
    if (bytesScanned > 0L) {
      builder.setSizeBytes(bytesScanned);
    }
    return builder.build();
  }

  private static IndexCoverage indexCoverage(
      FileTargetStats fileStats, List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    var coverage = IndexCoverage.newBuilder();
    if (fileStats.getRowCount() > 0L) {
      coverage.setRowsIndexed(fileStats.getRowCount());
      coverage.setLiveRowsIndexed(fileStats.getRowCount());
    }
    if (fileStats.getSizeBytes() > 0L) {
      coverage.setBytesScanned(fileStats.getSizeBytes());
    }
    if (pageIndexEntries != null && !pageIndexEntries.isEmpty()) {
      coverage.setPagesIndexed(pageIndexEntries.size());
      coverage.setRowGroupsIndexed(
          pageIndexEntries.stream()
              .map(FloecatConnector.ParquetPageIndexEntry::rowGroup)
              .distinct()
              .count());
    }
    return coverage.build();
  }

  private static String indexArtifactTargetStorageId(String filePath) {
    return "file:" + filePath;
  }

  private static String indexedColumnsProperty(
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    if (pageIndexEntries == null || pageIndexEntries.isEmpty()) {
      return "";
    }
    return pageIndexEntries.stream()
        .map(FloecatConnector.ParquetPageIndexEntry::columnName)
        .filter(name -> name != null && !name.isBlank())
        .map(String::trim)
        .distinct()
        .sorted()
        .collect(java.util.stream.Collectors.joining(","));
  }

  private static byte[] writeIndexSidecar(
      FileTargetStats fileStats, List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    var output = new InMemoryOutputFile();
    Configuration configuration = new Configuration(false);
    var groupFactory = new SimpleGroupFactory(BOOTSTRAP_INDEX_SIDECAR_SCHEMA);
    List<FloecatConnector.ParquetPageIndexEntry> sortedEntries =
        sortedPageIndexEntries(pageIndexEntries);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(output)
            .withConf(configuration)
            .withType(BOOTSTRAP_INDEX_SIDECAR_SCHEMA)
            .withExtraMetaData(Map.of("sidecar.data_file_path", fileStats.getFilePath()))
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      for (FloecatConnector.ParquetPageIndexEntry entry : sortedEntries) {
        writer.write(toGroup(groupFactory, entry));
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to write index sidecar for " + fileStats.getFilePath(), e);
    }
    return output.toByteArray();
  }

  private static Group toGroup(
      SimpleGroupFactory groupFactory, FloecatConnector.ParquetPageIndexEntry entry) {
    Group group =
        groupFactory
            .newGroup()
            .append("column_name", entry.columnName())
            .append("row_group", entry.rowGroup())
            .append("page_ordinal", entry.pageOrdinal())
            .append("first_row_index", entry.firstRowIndex())
            .append("row_count", entry.rowCount())
            .append("live_row_count", entry.liveRowCount())
            .append("page_total_compressed_size", entry.pageTotalCompressedSize())
            .append("requires_dictionary_page", entry.requiresDictionaryPage())
            .append("parquet_physical_type", entry.parquetPhysicalType())
            .append("parquet_compression", entry.parquetCompression())
            .append("parquet_max_def_level", (int) entry.parquetMaxDefLevel())
            .append("parquet_max_rep_level", (int) entry.parquetMaxRepLevel());
    if (entry.pageHeaderOffset() != null) {
      group.append("page_header_offset", entry.pageHeaderOffset());
    }
    if (entry.dictionaryPageHeaderOffset() != null) {
      group.append("dictionary_page_header_offset", entry.dictionaryPageHeaderOffset());
    }
    if (entry.dictionaryPageTotalCompressedSize() != null) {
      group.append(
          "dictionary_page_total_compressed_size", entry.dictionaryPageTotalCompressedSize());
    }
    if (entry.decimalPrecision() != null) {
      group.append("decimal_precision", entry.decimalPrecision());
    }
    if (entry.decimalScale() != null) {
      group.append("decimal_scale", entry.decimalScale());
    }
    if (entry.decimalBits() != null) {
      group.append("decimal_bits", entry.decimalBits());
    }
    if (entry.minI32() != null) {
      group.append("min_i32", entry.minI32());
    }
    if (entry.maxI32() != null) {
      group.append("max_i32", entry.maxI32());
    }
    if (entry.minI64() != null) {
      group.append("min_i64", entry.minI64());
    }
    if (entry.maxI64() != null) {
      group.append("max_i64", entry.maxI64());
    }
    if (entry.minF32() != null) {
      group.append("min_f32", entry.minF32());
    }
    if (entry.maxF32() != null) {
      group.append("max_f32", entry.maxF32());
    }
    if (entry.minF64() != null) {
      group.append("min_f64", entry.minF64());
    }
    if (entry.maxF64() != null) {
      group.append("max_f64", entry.maxF64());
    }
    if (entry.minBool() != null) {
      group.append("min_bool", entry.minBool());
    }
    if (entry.maxBool() != null) {
      group.append("max_bool", entry.maxBool());
    }
    if (entry.minUtf8() != null) {
      group.append("min_utf8", entry.minUtf8());
    }
    if (entry.maxUtf8() != null) {
      group.append("max_utf8", entry.maxUtf8());
    }
    if (entry.minDecimal128Unscaled() != null) {
      group.append(
          "min_decimal128_unscaled", Binary.fromConstantByteArray(entry.minDecimal128Unscaled()));
    }
    if (entry.maxDecimal128Unscaled() != null) {
      group.append(
          "max_decimal128_unscaled", Binary.fromConstantByteArray(entry.maxDecimal128Unscaled()));
    }
    if (entry.minDecimal256Unscaled() != null) {
      group.append(
          "min_decimal256_unscaled", Binary.fromConstantByteArray(entry.minDecimal256Unscaled()));
    }
    if (entry.maxDecimal256Unscaled() != null) {
      group.append(
          "max_decimal256_unscaled", Binary.fromConstantByteArray(entry.maxDecimal256Unscaled()));
    }
    return group;
  }

  private static List<FloecatConnector.ParquetPageIndexEntry> sortedPageIndexEntries(
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    if (pageIndexEntries == null || pageIndexEntries.isEmpty()) {
      return List.of();
    }
    return pageIndexEntries.stream()
        .sorted(
            Comparator.comparing(FloecatConnector.ParquetPageIndexEntry::columnName)
                .thenComparingInt(FloecatConnector.ParquetPageIndexEntry::rowGroup)
                .thenComparingInt(FloecatConnector.ParquetPageIndexEntry::pageOrdinal))
        .toList();
  }

  private static String sha256B64(byte[] bytes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return Base64.getEncoder().encodeToString(digest.digest(bytes));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to compute SHA-256", e);
    }
  }

  private static String base64ToHex(String base64) {
    byte[] raw = Base64.getDecoder().decode(base64);
    StringBuilder sb = new StringBuilder(raw.length * 2);
    for (byte value : raw) {
      sb.append(String.format("%02x", value));
    }
    return sb.toString();
  }

  private static String snapshotIndexSidecarBlobUri(
      String accountId, String tableId, long snapshotId, String targetId, String sha256) {
    return String.format(
        "/accounts/%s/tables/%s/index-sidecars/%019d/%s/%s.parquet",
        encode(accountId), encode(tableId), snapshotId, encode(targetId), encode(sha256));
  }

  private static String encode(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("index sidecar key segment is null/blank");
    }
    return ai.floedb.floecat.storage.kv.Keys.encodeSegment(value);
  }

  private static final class InMemoryOutputFile implements OutputFile {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    byte[] toByteArray() {
      return output.toByteArray();
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      output.reset();
      return new ByteArrayPositionOutputStream(output);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      output.reset();
      return new ByteArrayPositionOutputStream(output);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0L;
    }
  }

  private static final class ByteArrayPositionOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream output;

    ByteArrayPositionOutputStream(ByteArrayOutputStream output) {
      this.output = output;
    }

    @Override
    public long getPos() {
      return output.size();
    }

    @Override
    public void write(int b) {
      output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      output.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      output.flush();
    }

    @Override
    public void close() throws IOException {
      output.close();
    }
  }
}
