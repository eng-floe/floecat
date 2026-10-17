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

/** Describes how much ordered source history supports an identity state. */
public enum HistoryTier {
  /** An ordered, versioned transaction log was available. */
  VERSIONED_LOG,
  /** An ordered, versioned change feed was available. */
  VERSIONED_CHANGE_FEED,
  /** Historical schemas were available without reliable version ordering. */
  UNORDERED_QUERY_HISTORY,
  /** The source exposed only its current schema. */
  NONE;

  public boolean provesContinuity() {
    return this == VERSIONED_LOG || this == VERSIONED_CHANGE_FEED;
  }
}
