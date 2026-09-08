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

package ai.floedb.floecat.cache;

/**
 * The in-memory cache families that are independently budgeted and measured.
 *
 * <p>Each family is its own cache, never a tag inside a shared one: the values change at different
 * rates, and one eviction policy would let the fastest-moving family discard the slowest.
 *
 * <p>The tag is both the metric dimension and the config segment, so renaming a constant renames a
 * published series and an operator's property.
 */
public enum CacheFamily {

  /** Addressing: which object a name or id currently resolves to, and what a container holds. */
  POINTER("pointer");

  private final String tag;

  CacheFamily(String tag) {
    this.tag = tag;
  }

  /** Stable metric dimension and config segment. Not the enum name: this one is published. */
  public String tag() {
    return tag;
  }
}
