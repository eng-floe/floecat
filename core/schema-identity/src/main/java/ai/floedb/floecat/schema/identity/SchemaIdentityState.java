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

package ai.floedb.floecat.schema.identity;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/** Durable output of reconciling one complete source schema version. */
public final class SchemaIdentityState {
  private final long sourceVersion;
  private final long highWaterMark;
  private final IdentityMode mode;
  private final HistoryTier historyTier;
  private final OptionalLong nativeHighWaterMark;
  private final boolean sourceVersionAnchored;
  private final List<SchemaIdentityEntry> entries;
  private final String fingerprint;
  private final String stateChecksum;
  private final Map<ColumnPath, SchemaIdentityEntry> byPath;

  SchemaIdentityState(
      long sourceVersion,
      long highWaterMark,
      IdentityMode mode,
      HistoryTier historyTier,
      OptionalLong nativeHighWaterMark,
      boolean sourceVersionAnchored,
      List<SchemaIdentityEntry> entries,
      String fingerprint,
      String stateChecksum) {
    if (sourceVersion < 0) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    CanonicalColumnId.checkAllocatedRange(highWaterMark);
    this.sourceVersion = sourceVersion;
    this.highWaterMark = highWaterMark;
    this.mode = Objects.requireNonNull(mode, "mode");
    this.historyTier = Objects.requireNonNull(historyTier, "historyTier");
    this.nativeHighWaterMark = Objects.requireNonNull(nativeHighWaterMark, "nativeHighWaterMark");
    this.sourceVersionAnchored = sourceVersionAnchored;
    nativeHighWaterMark.ifPresent(CanonicalColumnId::checkAllocatedRange);
    if (mode != IdentityMode.NATIVE_FIELD_ID && nativeHighWaterMark.isPresent()) {
      throw new IllegalArgumentException("Native high-water mark requires native identity mode");
    }
    this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
    this.fingerprint = Objects.requireNonNull(fingerprint, "fingerprint");
    this.stateChecksum = Objects.requireNonNull(stateChecksum, "stateChecksum");
    this.byPath = new LinkedHashMap<>();
    Set<Long> canonicalIds = new HashSet<>();
    Set<Integer> nativeIds = new HashSet<>();
    for (SchemaIdentityEntry entry : this.entries) {
      SchemaIdentityEntry duplicate = byPath.putIfAbsent(entry.path(), entry);
      if (duplicate != null) {
        throw new IllegalArgumentException("Duplicate identity path " + entry.path().display());
      }
      if (mode == IdentityMode.NATIVE_FIELD_ID
          && entry.path().elements().getLast().kind() == NodeKind.FIELD) {
        int nativeId = entry.nativeFieldId().orElse(0);
        if (nativeId <= 0) {
          throw new IllegalArgumentException("Native identity field has no positive native ID");
        }
        if (!nativeIds.add(nativeId)) {
          throw new IllegalArgumentException("Duplicate native field ID " + nativeId);
        }
      }
      if (!CanonicalColumnId.isDerived(entry.canonicalId())
          && entry.canonicalId() > highWaterMark) {
        throw new IllegalArgumentException("Identity exceeds the high-water mark");
      }
      if (!canonicalIds.add(entry.canonicalId())) {
        throw new IllegalArgumentException("Duplicate canonical column ID " + entry.canonicalId());
      }
    }
  }

  public static SchemaIdentityState restore(
      long sourceVersion,
      long highWaterMark,
      IdentityMode mode,
      HistoryTier historyTier,
      OptionalLong nativeHighWaterMark,
      boolean sourceVersionAnchored,
      List<SchemaIdentityEntry> entries,
      String fingerprint,
      String stateChecksum) {
    SchemaIdentityState state =
        new SchemaIdentityState(
            sourceVersion,
            highWaterMark,
            mode,
            historyTier,
            nativeHighWaterMark,
            sourceVersionAnchored,
            entries,
            fingerprint,
            stateChecksum);
    String expectedFingerprint = SchemaIdentityReconciler.fingerprint(mode, entries);
    if (!expectedFingerprint.equals(fingerprint)) {
      throw new IllegalArgumentException("Column identity fingerprint does not match its mapping");
    }
    String expectedChecksum =
        SchemaIdentityReconciler.stateChecksum(
            fingerprint,
            highWaterMark,
            sourceVersion,
            nativeHighWaterMark,
            sourceVersionAnchored,
            historyTier,
            entries);
    if (!expectedChecksum.equals(stateChecksum)) {
      throw new IllegalArgumentException("Column identity state checksum does not match its state");
    }
    return state;
  }

  SchemaIdentityState copy(
      long sourceVersion,
      long highWaterMark,
      HistoryTier historyTier,
      OptionalLong nativeHighWaterMark,
      boolean sourceVersionAnchored) {
    if (this.sourceVersion == sourceVersion
        && this.highWaterMark == highWaterMark
        && this.historyTier == historyTier
        && this.nativeHighWaterMark.equals(nativeHighWaterMark)
        && this.sourceVersionAnchored == sourceVersionAnchored) {
      return this;
    }
    return new SchemaIdentityState(
        sourceVersion,
        highWaterMark,
        mode,
        historyTier,
        nativeHighWaterMark,
        sourceVersionAnchored,
        entries,
        fingerprint,
        SchemaIdentityReconciler.stateChecksum(
            fingerprint,
            highWaterMark,
            sourceVersion,
            nativeHighWaterMark,
            sourceVersionAnchored,
            historyTier,
            entries));
  }

  public long sourceVersion() {
    return sourceVersion;
  }

  /** Highest ID in the native or allocated space; deterministic derived IDs are excluded. */
  public long highWaterMark() {
    return highWaterMark;
  }

  public IdentityMode mode() {
    return mode;
  }

  public HistoryTier historyTier() {
    return historyTier;
  }

  public OptionalLong nativeHighWaterMark() {
    return nativeHighWaterMark;
  }

  public boolean sourceVersionAnchored() {
    return sourceVersionAnchored;
  }

  public List<SchemaIdentityEntry> entries() {
    return entries;
  }

  public String fingerprint() {
    return fingerprint;
  }

  public String stateChecksum() {
    return stateChecksum;
  }

  public Optional<SchemaIdentityEntry> byPath(ColumnPath path) {
    return Optional.ofNullable(byPath.get(path));
  }
}
