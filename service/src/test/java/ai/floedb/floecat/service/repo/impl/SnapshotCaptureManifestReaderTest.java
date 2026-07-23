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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestPage;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.rpc.FileGroupStatsPayload;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class SnapshotCaptureManifestReaderTest {
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("table-1")
          .build();

  @Test
  void statsAndIndexesResolveLazilyFromCaptureManifest() throws Exception {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    CountingBlobStore blobs = new CountingBlobStore();
    TableRootRepository roots = new TableRootRepository(pointers, blobs);
    SnapshotCaptureManifestReader reader = new SnapshotCaptureManifestReader(roots, blobs);
    String statsUri = "/accounts/acct/capture/group-1.stats.pb";
    String manifestUri = "/accounts/acct/capture/final.capture-manifest.pb";
    long snapshotId = 55L;
    TargetStatsRecord fileStats =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath("s3://data/file-1.parquet").build());
    FileGroupStatsPayload statsPack =
        FileGroupStatsPayload.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setTableId(TABLE_ID.getId())
            .setSnapshotId(snapshotId)
            .addFileStats(fileStats)
            .build();
    byte[] statsBytes = statsPack.toByteArray();
    byte[] fileStatsBytes = fileStats.toByteArray();
    int fileStatsOffset = indexOf(statsBytes, fileStatsBytes);
    assertTrue(fileStatsOffset >= 0);
    blobs.put(statsUri, statsBytes, "application/x-protobuf");
    IndexTarget indexTarget =
        IndexTarget.newBuilder()
            .setFile(IndexFileTarget.newBuilder().setFilePath("s3://data/file-1.parquet"))
            .build();
    IndexArtifactRecord index =
        IndexArtifactRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(snapshotId)
            .setTarget(indexTarget)
            .setArtifactUri("s3://indexes/file-1.parquet")
            .setArtifactFormat("parquet")
            .setArtifactFormatVersion(1)
            .setState(IndexArtifactState.IAS_READY)
            .build();
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setTableId(TABLE_ID.getId())
            .setSnapshotId(snapshotId)
            .addFileGroups(
                FileGroupResultDescriptor.newBuilder()
                    .setFormatVersion(1)
                    .setStatsPayloadUri(statsUri)
                    .setStatsPayloadBytes(statsBytes.length)
                    .setStatsPayloadSha256(ByteString.copyFrom(sha256(statsBytes)))
                    .setFileStatsRecordCount(1))
            .addStatsIndex(
                SnapshotCaptureManifest.StatsRecordLocation.newBuilder()
                    .setTargetStorageId(StatsTargetIdentity.storageId(fileStats.getTarget()))
                    .setPayloadUri(statsUri)
                    .setRecordIndex(0)
                    .setByteOffset(fileStatsOffset)
                    .setByteLength(fileStatsBytes.length)
                    .setRecordSha256(ByteString.copyFrom(sha256(fileStatsBytes))))
            .addIndexArtifacts(index)
            .build();
    byte[] manifestBytes = manifest.toByteArray();
    blobs.put(manifestUri, manifestBytes, "application/x-protobuf");
    BlobRef captureRef =
        BlobRef.newBuilder()
            .setUri(manifestUri)
            .setVersion(HexFormat.of().formatHex(sha256(manifestBytes)))
            .build();
    BlobRef pageRef =
        roots.putManifestPage(
            TABLE_ID.getAccountId(),
            TABLE_ID.getId(),
            SnapshotManifestPage.newBuilder()
                .addEntries(
                    SnapshotManifestEntry.newBuilder()
                        .setSnapshotId(snapshotId)
                        .setSnapshotRef(BlobRef.newBuilder().setUri("snapshot.pb").setVersion("v1"))
                        .setStatsGenerationRef(captureRef))
                .build());
    assertTrue(
        roots.createIfAbsent(
            TableRoot.newBuilder()
                .setTableId(TABLE_ID)
                .setCurrentSnapshotId(snapshotId)
                .setSnapshotManifestRef(pageRef)
                .build()));

    StatsRepository stats = new StatsRepository(pointers, blobs, null, reader);
    IndexArtifactRepository indexes = new IndexArtifactRepository(pointers, blobs, reader);

    assertEquals(
        fileStats, stats.getTargetStats(TABLE_ID, snapshotId, fileStats.getTarget()).orElseThrow());
    assertEquals(1, blobs.rangeReads);
    assertEquals(0, blobs.fullStatsPackReads);
    assertEquals(index, indexes.getIndexArtifact(TABLE_ID, snapshotId, indexTarget).orElseThrow());
    assertEquals(1, indexes.countIndexArtifacts(TABLE_ID, snapshotId));
  }

  @Test
  void rangeLessCaptureManifestFallsBackToVerifiedFullPackRead() throws Exception {
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    CountingBlobStore blobs = new CountingBlobStore();
    SnapshotCaptureManifestReader reader =
        new SnapshotCaptureManifestReader(new TableRootRepository(pointers, blobs), blobs);
    String statsUri = "/accounts/acct/capture/old-group.stats.pb";
    String manifestUri = "/accounts/acct/capture/old.capture-manifest.pb";
    long snapshotId = 56L;
    TargetStatsRecord fileStats =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath("s3://data/old-file.parquet").build());
    byte[] statsBytes =
        FileGroupStatsPayload.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setTableId(TABLE_ID.getId())
            .setSnapshotId(snapshotId)
            .addFileStats(fileStats)
            .build()
            .toByteArray();
    blobs.put(statsUri, statsBytes, "application/x-protobuf");
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setTableId(TABLE_ID.getId())
            .setSnapshotId(snapshotId)
            .addFileGroups(
                FileGroupResultDescriptor.newBuilder()
                    .setFormatVersion(1)
                    .setStatsPayloadUri(statsUri)
                    .setStatsPayloadBytes(statsBytes.length)
                    .setStatsPayloadSha256(ByteString.copyFrom(sha256(statsBytes)))
                    .setFileStatsRecordCount(1))
            .addStatsIndex(
                SnapshotCaptureManifest.StatsRecordLocation.newBuilder()
                    .setTargetStorageId(StatsTargetIdentity.storageId(fileStats.getTarget()))
                    .setPayloadUri(statsUri)
                    .setRecordIndex(0))
            .build();
    blobs.put(manifestUri, manifest.toByteArray(), "application/x-protobuf");

    assertEquals(
        fileStats,
        reader.getStats(manifestUri, TABLE_ID, snapshotId, fileStats.getTarget()).orElseThrow());
    assertEquals(0, blobs.rangeReads);
    assertEquals(1, blobs.fullStatsPackReads);
  }

  private static byte[] sha256(byte[] bytes) throws Exception {
    return MessageDigest.getInstance("SHA-256").digest(bytes);
  }

  private static int indexOf(byte[] haystack, byte[] needle) {
    outer:
    for (int offset = 0; offset <= haystack.length - needle.length; offset++) {
      for (int i = 0; i < needle.length; i++) {
        if (haystack[offset + i] != needle[i]) {
          continue outer;
        }
      }
      return offset;
    }
    return -1;
  }

  private static final class CountingBlobStore extends InMemoryBlobStore {
    int rangeReads;
    int fullStatsPackReads;

    @Override
    public byte[] get(String uri) {
      if (uri.endsWith(".stats.pb")) {
        fullStatsPackReads++;
      }
      return super.get(uri);
    }

    @Override
    public byte[] getRange(String uri, long offset, int length) {
      rangeReads++;
      return super.getRange(uri, offset, length);
    }
  }
}
