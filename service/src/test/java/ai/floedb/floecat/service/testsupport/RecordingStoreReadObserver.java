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

import ai.floedb.floecat.service.storage.ObservedBlobStore;
import ai.floedb.floecat.service.storage.ObservedPointerStore;
import ai.floedb.floecat.service.storage.StoreReadObserver;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Detailed local recorder selected by store-cost tests in place of the production observer. */
@Alternative
@Singleton
public final class RecordingStoreReadObserver implements StoreReadObserver {
  private enum FetchKind {
    GET,
    HEAD,
    LIST
  }

  private record Fetch(FetchKind kind, String target, String frames) {}

  private final List<String> pointerKeys = Collections.synchronizedList(new ArrayList<>());
  private final List<Fetch> blobFetches = Collections.synchronizedList(new ArrayList<>());
  private final AtomicInteger pointerGets = new AtomicInteger();
  private final AtomicInteger pointerBatches = new AtomicInteger();
  private final AtomicInteger pointerBatchKeys = new AtomicInteger();
  private final AtomicInteger pointerScans = new AtomicInteger();
  private final AtomicInteger pointerCounts = new AtomicInteger();
  private final AtomicInteger blobGets = new AtomicInteger();
  private final AtomicInteger blobBatches = new AtomicInteger();
  private final AtomicInteger blobBatchObjects = new AtomicInteger();
  private final AtomicInteger blobHeads = new AtomicInteger();
  private final AtomicInteger blobLists = new AtomicInteger();
  private final ByThread pointerThreads = new ByThread();
  private final ByThread blobThreads = new ByThread();

  @Override
  public boolean capturesTargets() {
    return true;
  }

  @Override
  public Observation begin(ReadCall call) {
    switch (call.store()) {
      case POINTER -> recordPointer(call);
      case BLOB -> recordBlob(call);
    }
    return Observation.NOOP;
  }

  private void recordPointer(ReadCall call) {
    switch (call.operation()) {
      case GET -> {
        pointerThreads.record();
        pointerGets.incrementAndGet();
        pointerKeys.addAll(call.targets());
      }
      case GET_BATCH -> {
        pointerThreads.record();
        pointerBatches.incrementAndGet();
        pointerBatchKeys.addAndGet(call.itemCount());
        pointerKeys.addAll(call.targets());
      }
      // The old in-memory counting subclass saw each consistent method delegate through its
      // corresponding ordinary method, so both names remain one legacy scan/count here.
      case SCAN_PREFIX, SCAN_PREFIX_CONSISTENT -> {
        pointerThreads.record();
        pointerScans.incrementAndGet();
      }
      case COUNT_PREFIX, COUNT_PREFIX_CONSISTENT -> {
        pointerThreads.record();
        pointerCounts.incrementAndGet();
      }
      // InMemoryPointerStore.isEmpty was never overridden by the old recorder. Production still
      // observes it, but adding it to this test unit would change the established cost formulas.
      case IS_EMPTY -> {}
      default -> throw new IllegalArgumentException("not a pointer operation: " + call.operation());
    }
  }

  private void recordBlob(ReadCall call) {
    FetchKind kind;
    switch (call.operation()) {
      case GET, GET_RANGE -> {
        blobThreads.record();
        blobGets.incrementAndGet();
        kind = FetchKind.GET;
      }
      case GET_BATCH -> {
        blobThreads.record();
        blobBatches.incrementAndGet();
        blobBatchObjects.addAndGet(call.itemCount());
        kind = FetchKind.GET;
      }
      // The old recorder inherited BlobStore.getRanges, whose default implementation called the
      // overridden singular get once per range. Preserve those units in the compatibility view.
      case GET_RANGES -> {
        blobThreads.record(call.itemCount());
        blobGets.addAndGet(call.itemCount());
        kind = FetchKind.GET;
      }
      case HEAD -> {
        blobThreads.record();
        blobHeads.incrementAndGet();
        kind = FetchKind.HEAD;
      }
      case LIST, LIST_PREFIXES -> {
        blobThreads.record();
        blobLists.incrementAndGet();
        kind = FetchKind.LIST;
      }
      default -> throw new IllegalArgumentException("not a blob operation: " + call.operation());
    }
    String frames = origin();
    call.targets().forEach(target -> blobFetches.add(new Fetch(kind, target, frames)));
  }

  public int pointerRoundTrips() {
    return pointerGets.get() + pointerBatches.get() + pointerScans.get() + pointerCounts.get();
  }

  public int pointerKeysRead() {
    return pointerGets.get() + pointerBatchKeys.get();
  }

  public int pointerPrefixWalks() {
    return pointerScans.get() + pointerCounts.get();
  }

  public int blobObjectGets() {
    return blobGets.get() + blobBatchObjects.get();
  }

  public int blobRoundTrips() {
    return blobGets.get() + blobBatches.get() + blobHeads.get() + blobLists.get();
  }

  public int blobHeads() {
    return blobHeads.get();
  }

  public int blobListCalls() {
    return blobLists.get();
  }

  void resetCounts() {
    pointerKeys.clear();
    blobFetches.clear();
    pointerThreads.clear();
    blobThreads.clear();
    pointerGets.set(0);
    pointerBatches.set(0);
    pointerBatchKeys.set(0);
    pointerScans.set(0);
    pointerCounts.set(0);
    blobGets.set(0);
    blobBatches.set(0);
    blobBatchObjects.set(0);
    blobHeads.set(0);
    blobLists.set(0);
  }

  void appendTo(StringBuilder out) {
    appendPointerTo(out);
    appendBlobTo(out);
  }

  private void appendPointerTo(StringBuilder out) {
    out.append("KV       roundTrips=")
        .append(pointerRoundTrips())
        .append("  keys=")
        .append(pointerKeysRead())
        .append("         gets=")
        .append(pointerGets.get())
        .append("  batchGets=")
        .append(pointerBatches.get())
        .append("  prefixScans=")
        .append(pointerScans.get())
        .append("  prefixCounts=")
        .append(pointerCounts.get())
        .append('\n');
    synchronized (pointerKeys) {
      if (!pointerKeys.isEmpty()) {
        out.append("pointer keys read (").append(pointerKeys.size()).append(")\n");
        pointerKeys.forEach(key -> out.append("  ").append(key).append('\n'));
      }
    }
    pointerThreads.appendTo(out, "  [kv] ");
  }

  private void appendBlobTo(StringBuilder out) {
    out.append("S3       objectGets=")
        .append(blobObjectGets())
        .append("  roundTrips=")
        .append(blobRoundTrips())
        .append("  gets=")
        .append(blobGets.get())
        .append("  batches=")
        .append(blobBatches.get())
        .append("  heads=")
        .append(blobHeads.get())
        .append("  listCalls=")
        .append(blobListCalls())
        .append('\n');
    synchronized (blobFetches) {
      if (!blobFetches.isEmpty()) {
        out.append("blob fetches (").append(blobFetches.size()).append(")\n");
        blobFetches.forEach(
            fetch ->
                out.append("  ")
                    .append(fetch.kind())
                    .append(' ')
                    .append(fetch.target())
                    .append('\n')
                    .append(fetch.frames()));
      }
    }
    blobThreads.appendTo(out, "  [s3] ");
  }

  private static final int ORIGIN_FRAMES = 14;

  private static String origin() {
    StringBuilder trace = new StringBuilder();
    StackWalker.getInstance()
        .walk(
            frames ->
                frames
                    .filter(frame -> frame.getClassName().startsWith("ai.floedb.floecat"))
                    .filter(frame -> !isInstrumentation(frame.getClassName()))
                    .limit(ORIGIN_FRAMES)
                    .toList())
        .forEach(frame -> trace.append("      at ").append(frame).append('\n'));
    return trace.toString();
  }

  private static boolean isInstrumentation(String className) {
    return className.startsWith(RecordingStoreReadObserver.class.getName())
        || className.startsWith(ObservedPointerStore.class.getName())
        || className.startsWith(ObservedBlobStore.class.getName())
        || className.equals("ai.floedb.floecat.service.storage.StoreReadInstrumentation");
  }
}
