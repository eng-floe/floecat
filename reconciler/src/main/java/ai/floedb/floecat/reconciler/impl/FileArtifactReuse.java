/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Defines and validates the immutable identity used for cross-snapshot file artifact reuse. */
public final class FileArtifactReuse {
  public static final String SOURCE_FINGERPRINT_PROPERTY = "floedb.reconcile.source-fingerprint-v1";
  public static final String STATS_SIGNATURE_PROPERTY = "floedb.reconcile.stats-signature-v1";
  public static final String INDEX_SIGNATURE_PROPERTY = "floedb.reconcile.index-signature-v1";
  public static final String REALIZED_STATS_SELECTORS_PROPERTY =
      "floedb.reconcile.realized-stats-selectors-v1";

  private FileArtifactReuse() {}

  public static String sourceFingerprint(
      ReconcileFileExecutionPlan plan, String executionSchemaJson) {
    DigestBuilder digest = new DigestBuilder("file-source-v1");
    digest.add(plan.filePath());
    digest.add(plan.fileFormat());
    digest.add(plan.fileSizeInBytes());
    digest.add(plan.contentIdentity());
    digest.add(plan.partitionDataJson());
    digest.add(plan.partitionSpecId());
    digest.add(executionSchemaJson);
    ReconcileFileExecutionPlan.DeltaDeletionVector dv = plan.deletionVector();
    digest.add(dv != null);
    if (dv != null) {
      digest.add(dv.storageType());
      digest.add(dv.pathOrInlineDv());
      digest.add(dv.offset() == null ? "" : Integer.toString(dv.offset()));
      digest.add(dv.sizeInBytes());
      digest.add(dv.cardinality());
    }
    Map<String, String> auxiliaryFingerprints = auxiliaryStatsFingerprints(plan);
    digest.add(auxiliaryFingerprints.size());
    auxiliaryFingerprints.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              digest.add(entry.getKey());
              digest.add(entry.getValue());
            });
    return digest.finish();
  }

  public static String indexSourceFingerprint(ReconcileFileExecutionPlan plan) {
    DigestBuilder digest = new DigestBuilder("file-index-source-v1");
    digest.add(plan.filePath());
    digest.add(plan.fileFormat());
    digest.add(plan.fileSizeInBytes());
    digest.add(plan.contentIdentity());
    return digest.finish();
  }

  public static Map<String, String> auxiliaryStatsFingerprints(ReconcileFileExecutionPlan plan) {
    java.util.LinkedHashMap<String, String> fingerprints = new java.util.LinkedHashMap<>();
    ReconcileFileExecutionPlan.DeltaDeletionVector dv = plan.deletionVector();
    if (dv != null && dv.onDisk()) {
      fingerprints.put(
          dv.pathOrInlineDv(),
          new DigestBuilder("delta-dv-v1")
              .addAndReturn(dv.pathOrInlineDv())
              .addAndReturn(dv.storageType())
              .addAndReturn(dv.offset() == null ? "" : Integer.toString(dv.offset()))
              .addAndReturn(dv.sizeInBytes())
              .addAndReturn(dv.cardinality())
              .finish());
    }
    for (ReconcileFileExecutionPlan.IcebergDeleteFile delete : plan.icebergDeleteFiles()) {
      DigestBuilder digest = new DigestBuilder("iceberg-delete-v1");
      digest.add(delete.filePath());
      digest.add(delete.fileSizeInBytes());
      digest.add(delete.contentIdentity());
      digest.add(delete.content().name());
      digest.add(delete.partitionSpecId());
      delete.equalityFieldIds().stream().sorted().forEach(digest::add);
      fingerprints.put(delete.filePath(), digest.finish());
    }
    return Map.copyOf(fingerprints);
  }

  public static String statsCaptureSignature(ReconcileCapturePolicy policy) {
    return captureSignature(policy, true, "");
  }

  public static String indexCaptureSignature(
      ReconcileCapturePolicy policy, String executionSchemaJson) {
    return captureSignature(policy, false, executionSchemaJson);
  }

  private static String captureSignature(
      ReconcileCapturePolicy policy, boolean stats, String executionSchemaJson) {
    ReconcileCapturePolicy effective = policy == null ? ReconcileCapturePolicy.empty() : policy;
    DigestBuilder digest = new DigestBuilder(stats ? "stats-capture" : "index-capture");
    digest.add("default-column-scope");
    digest.add(effective.defaultColumnScope().name());
    digest.add("max-default-columns");
    digest.add(effective.maxDefaultColumns());

    List<String> selectors =
        effective.columns().stream()
            .filter(column -> stats ? column.captureStats() : column.captureIndex())
            .sorted(Comparator.comparing(ReconcileCapturePolicy.Column::selector))
            .map(ReconcileCapturePolicy.Column::selector)
            .toList();
    digest.add("selectors");
    digest.add(selectors.size());
    selectors.forEach(digest::add);

    List<String> outputs =
        effective.outputs().stream()
            .filter(
                output ->
                    stats
                        ? output != ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX
                        : output == ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)
            .sorted()
            .map(Enum::name)
            .toList();
    digest.add("outputs");
    digest.add(outputs.size());
    outputs.forEach(digest::add);

    List<Map.Entry<String, String>> properties =
        effective.properties().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
    digest.add("properties");
    digest.add(properties.size());
    properties.forEach(
        entry -> {
          digest.add(entry.getKey());
          digest.add(entry.getValue());
        });
    if (!stats
        && effective.requestsIndexes()
        && effective.selectorsForIndex().isEmpty()
        && effective.defaultColumnScope()
            != ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY) {
      digest.add("default-execution-schema");
      digest.add(executionSchemaJson);
    }
    return digest.finish();
  }

  public static boolean compatibleStats(
      TargetStatsRecord record,
      String expectedFilePath,
      String sourceFingerprint,
      String statsSignature) {
    return record != null
        && record.hasFile()
        && record.hasTarget()
        && record.getTarget().hasFile()
        && expectedFilePath.equals(record.getTarget().getFile().getFilePath())
        && expectedFilePath.equals(record.getFile().getFilePath())
        && sourceFingerprint.equals(record.getPropertiesMap().get(SOURCE_FINGERPRINT_PROPERTY))
        && statsSignature.equals(record.getPropertiesMap().get(STATS_SIGNATURE_PROPERTY))
        && record.getPropertiesMap().containsKey(REALIZED_STATS_SELECTORS_PROPERTY);
  }

  public static boolean compatibleIndex(
      IndexArtifactRecord record,
      String expectedFilePath,
      String sourceFingerprint,
      String indexSignature) {
    return record != null
        && record.hasTarget()
        && record.getTarget().hasFile()
        && expectedFilePath.equals(record.getTarget().getFile().getFilePath())
        && !record.getArtifactUri().isBlank()
        && record.getState() == IndexArtifactState.IAS_READY
        && sourceFingerprint.equals(record.getPropertiesMap().get(SOURCE_FINGERPRINT_PROPERTY))
        && indexSignature.equals(record.getPropertiesMap().get(INDEX_SIGNATURE_PROPERTY))
        && !record.getPropertiesMap().getOrDefault("indexed_columns", "").isBlank();
  }

  public static TargetStatsRecord bindStatsToSnapshot(
      TargetStatsRecord prior,
      ResourceId tableId,
      long snapshotId,
      String sourceFingerprint,
      String statsSignature) {
    var file = prior.getFile().toBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    List<FileColumnStats> columns = new ArrayList<>(prior.getFile().getColumnsCount());
    for (FileColumnStats column : prior.getFile().getColumnsList()) {
      var next = column.toBuilder();
      if (column.hasScalar() && column.getScalar().hasUpstream()) {
        next.setScalar(
            column.getScalar().toBuilder()
                .setUpstream(
                    column.getScalar().getUpstream().toBuilder()
                        .setCommitRef(Long.toString(snapshotId))));
      }
      columns.add(next.build());
    }
    file.clearColumns().addAllColumns(columns);
    return prior.toBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setFile(file)
        .putProperties(SOURCE_FINGERPRINT_PROPERTY, sourceFingerprint)
        .putProperties(STATS_SIGNATURE_PROPERTY, statsSignature)
        .build();
  }

  public static TargetStatsRecord auxiliaryStatsRecord(
      ReconcileFileExecutionPlan plan,
      String filePath,
      ResourceId tableId,
      long snapshotId,
      String sourceFingerprint,
      String statsSignature) {
    FileTargetStats.Builder file =
        FileTargetStats.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setFilePath(filePath);
    ReconcileFileExecutionPlan.DeltaDeletionVector dv = plan.deletionVector();
    if (dv != null && dv.onDisk() && dv.pathOrInlineDv().equals(filePath)) {
      file.setRowCount(dv.cardinality())
          .setSizeBytes(dv.sizeInBytes())
          .setFileContent(FileContent.FC_POSITION_DELETES);
    } else {
      ReconcileFileExecutionPlan.IcebergDeleteFile delete =
          plan.icebergDeleteFiles().stream()
              .filter(candidate -> candidate.filePath().equals(filePath))
              .findFirst()
              .orElseThrow(
                  () -> new IllegalArgumentException("unknown auxiliary stats path " + filePath));
      IcebergContentIdentity identity = parseIcebergContentIdentity(delete.contentIdentity());
      file.setRowCount(identity.recordCount())
          .setSizeBytes(delete.fileSizeInBytes())
          .setFileContent(
              switch (delete.content()) {
                case POSITION -> FileContent.FC_POSITION_DELETES;
                case EQUALITY -> FileContent.FC_EQUALITY_DELETES;
                case UNSPECIFIED -> FileContent.FC_UNSPECIFIED;
              })
          .setPartitionSpecId(delete.partitionSpecId())
          .addAllEqualityFieldIds(delete.equalityFieldIds());
      if (identity.sequenceNumber() != null && identity.sequenceNumber() > 0L) {
        file.setSequenceNumber(identity.sequenceNumber());
      }
    }
    TargetStatsRecord record =
        TargetStatsRecord.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath(filePath)))
            .setFile(file)
            .build();
    return stampStats(record, sourceFingerprint, statsSignature, List.of());
  }

  private static IcebergContentIdentity parseIcebergContentIdentity(String encoded) {
    if (encoded == null || !encoded.startsWith("iceberg-") || !encoded.contains("-v1:")) {
      return new IcebergContentIdentity(null, 0L);
    }
    String[] parts = encoded.substring(encoded.indexOf("-v1:") + 4).split(":", -1);
    try {
      Long sequence = parts.length > 0 && !parts[0].isBlank() ? Long.valueOf(parts[0]) : null;
      long records = parts.length > 1 && !parts[1].isBlank() ? Long.parseLong(parts[1]) : 0L;
      return new IcebergContentIdentity(sequence, Math.max(0L, records));
    } catch (NumberFormatException ignored) {
      return new IcebergContentIdentity(null, 0L);
    }
  }

  private record IcebergContentIdentity(Long sequenceNumber, long recordCount) {}

  public static IndexArtifactRecord bindIndexToSnapshot(
      IndexArtifactRecord prior,
      ResourceId tableId,
      long snapshotId,
      String sourceFingerprint,
      String indexSignature) {
    return prior.toBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .putProperties(SOURCE_FINGERPRINT_PROPERTY, sourceFingerprint)
        .putProperties(INDEX_SIGNATURE_PROPERTY, indexSignature)
        .build();
  }

  public static TargetStatsRecord stampStats(
      TargetStatsRecord record,
      String sourceFingerprint,
      String statsSignature,
      List<String> realizedSelectors) {
    return record.toBuilder()
        .putProperties(SOURCE_FINGERPRINT_PROPERTY, sourceFingerprint)
        .putProperties(STATS_SIGNATURE_PROPERTY, statsSignature)
        .putProperties(
            REALIZED_STATS_SELECTORS_PROPERTY,
            String.join(",", realizedSelectors == null ? List.of() : realizedSelectors))
        .build();
  }

  public static IndexArtifactRecord stampIndex(
      IndexArtifactRecord record, String sourceFingerprint, String indexSignature) {
    return record.toBuilder()
        .putProperties(SOURCE_FINGERPRINT_PROPERTY, sourceFingerprint)
        .putProperties(INDEX_SIGNATURE_PROPERTY, indexSignature)
        .build();
  }

  private static final class DigestBuilder {
    private final MessageDigest digest;

    private DigestBuilder(String domain) {
      try {
        digest = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
      add(domain);
    }

    private void add(String value) {
      byte[] bytes = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
      digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
      digest.update(bytes);
    }

    private void add(long value) {
      digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private void add(int value) {
      digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private void add(boolean value) {
      digest.update((byte) (value ? 1 : 0));
    }

    private DigestBuilder addAndReturn(String value) {
      add(value);
      return this;
    }

    private DigestBuilder addAndReturn(long value) {
      add(value);
      return this;
    }

    private DigestBuilder addAndReturn(int value) {
      add(value);
      return this;
    }

    private String finish() {
      return HexFormat.of().formatHex(digest.digest());
    }
  }
}
