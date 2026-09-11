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
import java.util.Set;

/** Durable output of reconciling one complete source schema version. */
public final class SchemaIdentityState {
  private final long sourceVersion;
  private final long highWaterMark;
  private final IdentityMode mode;
  private final List<SchemaIdentityEntry> entries;
  private final String fingerprint;
  private final Map<ColumnPath, SchemaIdentityEntry> byPath;

  SchemaIdentityState(
      long sourceVersion,
      long highWaterMark,
      IdentityMode mode,
      List<SchemaIdentityEntry> entries,
      String fingerprint) {
    if (sourceVersion < 0) {
      throw new IllegalArgumentException("Source version must be non-negative");
    }
    if (highWaterMark < 0) {
      throw new IllegalArgumentException("High-water mark must be non-negative");
    }
    this.sourceVersion = sourceVersion;
    this.highWaterMark = highWaterMark;
    this.mode = Objects.requireNonNull(mode, "mode");
    this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
    this.fingerprint = Objects.requireNonNull(fingerprint, "fingerprint");
    this.byPath = new LinkedHashMap<>();
    Set<Long> canonicalIds = new HashSet<>();
    for (SchemaIdentityEntry entry : this.entries) {
      SchemaIdentityEntry duplicate = byPath.putIfAbsent(entry.path(), entry);
      if (duplicate != null) {
        throw new IllegalArgumentException("Duplicate identity path " + entry.path().display());
      }
      if (entry.canonicalId() > highWaterMark) {
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
      List<SchemaIdentityEntry> entries,
      String fingerprint) {
    SchemaIdentityState state =
        new SchemaIdentityState(sourceVersion, highWaterMark, mode, entries, fingerprint);
    String expected = SchemaIdentityReconciler.fingerprint(mode, highWaterMark, entries);
    if (!expected.equals(fingerprint)) {
      throw new IllegalArgumentException("Column identity fingerprint does not match its mapping");
    }
    return state;
  }

  public long sourceVersion() {
    return sourceVersion;
  }

  public long highWaterMark() {
    return highWaterMark;
  }

  public IdentityMode mode() {
    return mode;
  }

  public List<SchemaIdentityEntry> entries() {
    return entries;
  }

  public String fingerprint() {
    return fingerprint;
  }

  public Optional<SchemaIdentityEntry> byPath(ColumnPath path) {
    return Optional.ofNullable(byPath.get(path));
  }
}
