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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Test helpers for snapshot-related fixtures. */
public final class SnapshotTestSupport {

  private SnapshotTestSupport() {}

  public static final class FakeSnapshotRepository extends SnapshotRepository {

    private final Map<Long, Snapshot> snapshots = new HashMap<>();

    public FakeSnapshotRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    public void put(ResourceId tableId, Snapshot snapshot) {
      put(snapshot);
    }

    public void put(Snapshot snapshot) {
      snapshots.put(snapshot.getSnapshotId(), snapshot);
    }

    @Override
    public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
      return Optional.ofNullable(snapshots.get(snapshotId));
    }

    @Override
    public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
      return snapshots.values().stream().max(Comparator.comparingLong(this::createdMillis));
    }

    @Override
    public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
      long target = asOf.getSeconds();
      return snapshots.values().stream()
          .filter(s -> s.getUpstreamCreatedAt().getSeconds() <= target)
          .max(Comparator.comparingLong(this::createdMillis));
    }

    private long createdMillis(Snapshot snapshot) {
      Timestamp ts = snapshot.getUpstreamCreatedAt();
      return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
    }
  }

  public static final class FakeSnapshotClient implements SnapshotHelper.SnapshotClient {

    public GetSnapshotResponse nextResponse;
    public GetSnapshotRequest lastRequest;

    @Override
    public GetSnapshotResponse getSnapshot(GetSnapshotRequest request) {
      lastRequest = request;
      if (nextResponse == null) {
        throw new IllegalStateException("no snapshot response configured");
      }
      return nextResponse;
    }
  }
}
