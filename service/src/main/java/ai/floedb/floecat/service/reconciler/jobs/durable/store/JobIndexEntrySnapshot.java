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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

/**
 * Read-only view of a job index entry. {@code cleanupLocked} is set only for a canonical entry
 * whose durable deletion claim has already been acquired.
 */
public record JobIndexEntrySnapshot(
    String pointerKey,
    String blobUri,
    long version,
    String lookupStoragePartitionKey,
    boolean cleanupLocked) {
  public JobIndexEntrySnapshot(String pointerKey, String blobUri, long version) {
    this(pointerKey, blobUri, version, "", false);
  }

  public JobIndexEntrySnapshot(
      String pointerKey, String blobUri, long version, String lookupStoragePartitionKey) {
    this(pointerKey, blobUri, version, lookupStoragePartitionKey, false);
  }

  public JobIndexEntrySnapshot(
      String pointerKey, String blobUri, long version, boolean cleanupLocked) {
    this(pointerKey, blobUri, version, "", cleanupLocked);
  }

  public JobIndexEntrySnapshot {
    lookupStoragePartitionKey = lookupStoragePartitionKey == null ? "" : lookupStoragePartitionKey;
  }
}
