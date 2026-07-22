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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.RelationResolutions;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsBundleEnd;
import ai.floedb.floecat.query.rpc.UserObjectsBundleHeader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * The GetUserObjects wire protocol for one request: a header chunk, then zero or more resolution
 * chunks, then a single end chunk carrying the result counts — every chunk stamped with a monotonic
 * {@code seq}. Resolutions are buffered and sliced so no chunk exceeds {@code
 * maxResolutionsPerChunk}.
 *
 * <p>The pipeline hands this framer already-built {@link RelationResolution}s in emit order via
 * {@link #offer}; the framer only frames the sequence it is given — it never reorders. It owns the
 * protocol invariants (header once and first, end once and last, monotonic seq, chunk-size cap) so
 * the iterator driving it does not track any of them. It knows nothing of pins, stats, building, or
 * telemetry; the driver decides the batch boundary (which governs the pin/stats/build barrier) and
 * passes the final counts to {@link #end}.
 *
 * <p>Not thread-safe: one stream drives one request from a single iterator.
 */
final class BundleChunkStream {

  private final String queryId;
  private final int maxResolutionsPerChunk;
  private final ArrayDeque<RelationResolution> buffered = new ArrayDeque<>();
  private int seq = 1;
  private boolean headerEmitted = false;
  private boolean endEmitted = false;

  BundleChunkStream(String queryId, int maxResolutionsPerChunk) {
    this.queryId = queryId;
    this.maxResolutionsPerChunk = maxResolutionsPerChunk;
  }

  /** True until the end chunk has been emitted — the stream still owes at least the end. */
  boolean isOpen() {
    return !endEmitted;
  }

  /** True before the header chunk has been emitted. */
  boolean headerPending() {
    return !headerEmitted;
  }

  /** Emit the header chunk. Must be the first chunk; call only when {@link #headerPending}. */
  UserObjectsBundleChunk header() {
    headerEmitted = true;
    UserObjectsBundleHeader header = UserObjectsBundleHeader.newBuilder().build();
    return UserObjectsBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq++)
        .setHeader(header)
        .build();
  }

  /** Append built resolutions, in emit order, to the buffer awaiting framing. */
  void offer(List<RelationResolution> resolutions) {
    buffered.addAll(resolutions);
  }

  /** True while resolutions remain to be framed into a chunk. */
  boolean hasBufferedResolutions() {
    return !buffered.isEmpty();
  }

  /**
   * Frame the next resolution chunk: up to {@code maxResolutionsPerChunk} buffered resolutions, in
   * order, stamped with the next seq. Call only when {@link #hasBufferedResolutions}.
   */
  UserObjectsBundleChunk nextResolutionChunk() {
    List<RelationResolution> slice =
        new ArrayList<>(Math.min(maxResolutionsPerChunk, buffered.size()));
    while (slice.size() < maxResolutionsPerChunk && !buffered.isEmpty()) {
      slice.add(buffered.removeFirst());
    }
    RelationResolutions chunk = RelationResolutions.newBuilder().addAllItems(slice).build();
    return UserObjectsBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq++)
        .setResolutions(chunk)
        .build();
  }

  /** Emit the end chunk with the request's result counts. Terminal; call once, last. */
  UserObjectsBundleChunk end(int resolutionCount, int foundCount, int notFoundCount) {
    endEmitted = true;
    UserObjectsBundleEnd end =
        UserObjectsBundleEnd.newBuilder()
            .setResolutionCount(resolutionCount)
            .setFoundCount(foundCount)
            .setNotFoundCount(notFoundCount)
            .build();
    return UserObjectsBundleChunk.newBuilder()
        .setQueryId(queryId)
        .setSeq(seq++)
        .setEnd(end)
        .build();
  }

  /** The seq the next emitted chunk will carry — for logging only. */
  int seq() {
    return seq;
  }
}
