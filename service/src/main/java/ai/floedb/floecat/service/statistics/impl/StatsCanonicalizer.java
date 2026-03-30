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

package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.NdvSketch;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;

/** Canonical fingerprint utilities for target stats idempotency checks. */
final class StatsCanonicalizer {

  private StatsCanonicalizer() {}

  static byte[] canonicalFingerprint(TargetStatsRecord record) {
    var c = new Canonicalizer();
    canonicalResourceId(c, "table_id", record.getTableId());
    c.scalar("snapshot_id", record.getSnapshotId());
    c.scalar("target", StatsTargetIdentity.storageId(record.getTarget()));
    canonicalStatsMetadata(c, "metadata", record.hasMetadata() ? record.getMetadata() : null);
    if (record.hasTable()) {
      c.group(
          "table_value",
          g -> {
            var table = record.getTable();
            canonicalUpstream(g, "upstream", table.hasUpstream() ? table.getUpstream() : null);
            g.scalar("row_count", table.getRowCount());
            g.scalar("data_file_count", table.getDataFileCount());
            g.scalar("total_size_bytes", table.getTotalSizeBytes());
            g.map("properties", table.getPropertiesMap());
          });
    }
    if (record.hasScalar()) {
      c.group(
          "scalar_value",
          g -> {
            var scalar = record.getScalar();
            g.scalar("logical_type", scalar.getLogicalType());
            canonicalUpstream(g, "upstream", scalar.hasUpstream() ? scalar.getUpstream() : null);
            g.scalar("value_count", scalar.getValueCount());
            g.scalar("null_count", scalar.getNullCount());
            g.scalar("nan_count", scalar.getNanCount());
            canonicalNdv(g, "ndv", scalar.hasNdv() ? scalar.getNdv() : null);
            g.scalar("min", scalar.getMin());
            g.scalar("max", scalar.getMax());
            g.scalar("histogram_b64", bytesToB64(scalar.getHistogram().toByteArray()));
            g.scalar("tdigest_b64", bytesToB64(scalar.getTdigest().toByteArray()));
            g.map("properties", scalar.getPropertiesMap());
          });
    }
    if (record.hasFile()) {
      c.group(
          "file_value",
          g -> g.scalar("fp_b64", bytesToB64(canonicalFingerprint(record.getFile()))));
    }
    c.map("properties", record.getPropertiesMap());
    return c.bytes();
  }

  static byte[] canonicalFingerprint(ScalarStats stats) {
    var c = new Canonicalizer();
    c.scalar("logical_type", stats.getLogicalType());
    canonicalUpstream(c, "upstream", stats.hasUpstream() ? stats.getUpstream() : null);
    c.scalar("value_count", stats.getValueCount());
    c.scalar("null_count", stats.getNullCount());
    c.scalar("nan_count", stats.getNanCount());
    canonicalNdv(c, "ndv", stats.hasNdv() ? stats.getNdv() : null);
    c.scalar("min", stats.getMin());
    c.scalar("max", stats.getMax());
    c.scalar("histogram_b64", bytesToB64(stats.getHistogram().toByteArray()));
    c.scalar("tdigest_b64", bytesToB64(stats.getTdigest().toByteArray()));
    c.map("properties", stats.getPropertiesMap());
    return c.bytes();
  }

  static byte[] canonicalFingerprint(FileTargetStats stats) {
    var c = new Canonicalizer();
    canonicalResourceId(c, "table_id", stats.getTableId());
    c.scalar("snapshot_id", stats.getSnapshotId());
    c.scalar("file_path", stats.getFilePath());
    c.scalar("file_format", stats.getFileFormat());
    c.scalar("row_count", stats.getRowCount());
    c.scalar("size_bytes", stats.getSizeBytes());
    c.scalar("file_content", stats.getFileContent().name());
    c.scalar("partition_data_json", stats.getPartitionDataJson());
    c.scalar("partition_spec_id", stats.getPartitionSpecId());
    if (stats.hasSequenceNumber()) {
      c.scalar("sequence_number", stats.getSequenceNumber());
    }

    var eqIds = new ArrayList<Integer>(stats.getEqualityFieldIdsList());
    eqIds.sort(Comparator.naturalOrder());
    c.list("equality_field_ids", eqIds);

    var cols = new ArrayList<>(stats.getColumnsList());
    cols.sort(
        Comparator.comparing(ScalarStats::getLogicalType)
            .thenComparing(col -> bytesToB64(canonicalFingerprint(col))));
    for (int i = 0; i < cols.size(); i++) {
      ScalarStats col = cols.get(i);
      c.group("column_" + i, g -> g.scalar("fp_b64", bytesToB64(canonicalFingerprint(col))));
    }

    return c.bytes();
  }

  private static void canonicalResourceId(Canonicalizer c, String key, ResourceId id) {
    c.group(
        key,
        g -> {
          if (id == null) {
            return;
          }
          g.scalar("account_id", id.getAccountId());
          g.scalar("id", id.getId());
          g.scalar("kind", id.getKind().name());
        });
  }

  private static void canonicalUpstream(Canonicalizer c, String key, UpstreamStamp up) {
    if (up == null) {
      return;
    }
    c.group(
        key,
        g -> {
          g.scalar("system", up.getSystem().name());
          g.scalar("table_native_id", up.getTableNativeId());
          g.scalar("commit_ref", up.getCommitRef());
          g.scalar("fetched_at_seconds", up.hasFetchedAt() ? up.getFetchedAt().getSeconds() : 0L);
          g.scalar("fetched_at_nanos", up.hasFetchedAt() ? up.getFetchedAt().getNanos() : 0);
          g.map("properties", up.getPropertiesMap());
        });
  }

  private static void canonicalNdv(Canonicalizer c, String key, Ndv ndv) {
    if (ndv == null) {
      return;
    }
    c.group(
        key,
        g -> {
          g.scalar("mode", ndv.getModeCase().name());
          if (ndv.hasExact()) {
            g.scalar("exact", ndv.getExact());
          }
          if (ndv.hasApprox()) {
            canonicalNdvApprox(g, "approx", ndv.getApprox());
          }
          var sketches = new ArrayList<>(ndv.getSketchesList());
          sketches.sort(
              Comparator.comparing(NdvSketch::getType)
                  .thenComparing(NdvSketch::getEncoding)
                  .thenComparing(NdvSketch::getCompression)
                  .thenComparingInt(NdvSketch::getVersion)
                  .thenComparing(s -> bytesToB64(s.getData().toByteArray())));
          for (var sketch : sketches) {
            canonicalNdvSketch(g, "sketch_" + sketch.getType(), sketch);
          }
        });
  }

  private static void canonicalNdvApprox(Canonicalizer c, String key, NdvApprox approx) {
    c.group(
        key,
        g -> {
          g.scalar("estimate", approx.getEstimate());
          g.scalar("relative_standard_error", approx.getRelativeStandardError());
          g.scalar("confidence_lower", approx.getConfidenceLower());
          g.scalar("confidence_upper", approx.getConfidenceUpper());
          g.scalar("confidence_level", approx.getConfidenceLevel());
          g.scalar("rows_seen", approx.getRowsSeen());
          g.scalar("rows_total", approx.getRowsTotal());
          g.scalar("method", approx.getMethod());
          g.map("properties", approx.getPropertiesMap());
        });
  }

  private static void canonicalNdvSketch(Canonicalizer c, String key, NdvSketch sketch) {
    c.group(
        key,
        g -> {
          g.scalar("type", sketch.getType());
          g.scalar("encoding", sketch.getEncoding());
          g.scalar("compression", sketch.getCompression());
          g.scalar("version", sketch.getVersion());
          g.scalar("data_b64", bytesToB64(sketch.getData().toByteArray()));
          g.map("properties", sketch.getPropertiesMap());
        });
  }

  private static void canonicalStatsMetadata(Canonicalizer c, String key, StatsMetadata metadata) {
    if (metadata == null) {
      return;
    }
    c.group(
        key,
        g -> {
          g.scalar("producer", metadata.getProducer().name());
          g.scalar("completeness", metadata.getCompleteness().name());
          g.scalar("capture_mode", metadata.getCaptureMode().name());
          if (metadata.hasConfidenceLevel()) {
            g.scalar("confidence_level", metadata.getConfidenceLevel());
          }
          canonicalStatsCoverage(
              g, "coverage", metadata.hasCoverage() ? metadata.getCoverage() : null);
          // captured_at and refreshed_at are operational timestamps and intentionally excluded
          // from content identity/fingerprinting used by idempotency.
          g.map("properties", metadata.getPropertiesMap());
        });
  }

  private static void canonicalStatsCoverage(Canonicalizer c, String key, StatsCoverage coverage) {
    if (coverage == null) {
      return;
    }
    c.group(
        key,
        g -> {
          if (coverage.hasRowsScanned()) {
            g.scalar("rows_scanned", coverage.getRowsScanned());
          }
          if (coverage.hasFilesScanned()) {
            g.scalar("files_scanned", coverage.getFilesScanned());
          }
          if (coverage.hasRowGroupsSampled()) {
            g.scalar("row_groups_sampled", coverage.getRowGroupsSampled());
          }
          if (coverage.hasBytesScanned()) {
            g.scalar("bytes_scanned", coverage.getBytesScanned());
          }
          g.map("properties", coverage.getPropertiesMap());
        });
  }

  private static String bytesToB64(byte[] data) {
    if (data == null || data.length == 0) {
      return "";
    }
    return Base64.getEncoder().encodeToString(data);
  }
}
