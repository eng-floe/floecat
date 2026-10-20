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

package ai.floedb.floecat.catalog.delta;

import java.util.Map;

/** Distinguishes a confirmed Delta version, including zero, from an unversioned catalog schema. */
public record DeltaSourceVersion(long value, boolean anchored) {
  public static final String SHARING_VERSION = "delta.sharing.version";

  public DeltaSourceVersion {
    if (value < 0L) {
      throw new IllegalArgumentException("Delta source version must be non-negative");
    }
  }

  public static DeltaSourceVersion fromProperties(Map<String, String> properties) {
    String raw = properties.get(SHARING_VERSION);
    if (raw == null || raw.isBlank()) {
      return new DeltaSourceVersion(0L, false);
    }
    try {
      return new DeltaSourceVersion(Long.parseLong(raw), true);
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("Invalid Delta source version " + raw, invalid);
    }
  }
}
