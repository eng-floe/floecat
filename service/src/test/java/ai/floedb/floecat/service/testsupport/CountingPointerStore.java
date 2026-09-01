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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Alternative
@Singleton
public class CountingPointerStore extends InMemoryPointerStore {
  /**
   * The keys individually read, in order. A count says a cost was exceeded; the keys say by which
   * family, which is the difference between a real bypass and a fixture artefact.
   */
  private final List<String> observedKeys = Collections.synchronizedList(new ArrayList<>());

  private final AtomicInteger gets = new AtomicInteger();
  private final AtomicInteger batchGets = new AtomicInteger();

  /** Keys read through getBatch, counted individually. See {@link #roundTrips}. */
  private final AtomicInteger batchedKeys = new AtomicInteger();

  private final AtomicInteger prefixScans = new AtomicInteger();
  private final AtomicInteger prefixCounts = new AtomicInteger();

  /** The same reads, attributed to the thread that made them. */
  private final ByThread byThread = new ByThread();

  @Override
  public Optional<Pointer> get(String key) {
    observedKeys.add(key);
    gets.incrementAndGet();
    byThread.record();
    return super.get(key);
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    if (keys != null && !keys.isEmpty()) {
      batchGets.incrementAndGet();
      batchedKeys.addAndGet(keys.size());
      observedKeys.addAll(keys);
      byThread.record();
    }
    return super.getBatch(keys);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    prefixScans.incrementAndGet();
    byThread.record();
    return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public int countByPrefix(String prefix) {
    prefixCounts.incrementAndGet();
    byThread.record();
    return super.countByPrefix(prefix);
  }

  /**
   * Round trips this store served: what a request pays in latency.
   *
   * <p>A {@code getBatch} of eight keys is ONE round trip here and EIGHT keys in {@code keysRead}.
   * Both are real and they are not interchangeable -- a read path refactored from eight gets into
   * one batch does the same work and costs a seventh of the round trips, so a coefficient derived
   * in one unit and applied in the other is simply wrong. The blob side counts objects, which is
   * why neither is called "ops".
   */
  public int roundTrips() {
    return gets.get() + batchGets.get() + prefixScans.get() + prefixCounts.get();
  }

  /**
   * Range reads: listings and prefix counts, the calls that read a span rather than a key.
   *
   * <p>These are inside {@link #roundTrips} too, which is exactly why they need a number of their
   * own. A read path that replaced one get with one walk over the whole catalog holds the
   * round-trip total still while the cost grows with the catalog, so the total cannot gate it. Both
   * suites assert this is zero.
   */
  public int prefixWalks() {
    return prefixScans.get() + prefixCounts.get();
  }

  /** Individual pointer keys read, counting each key of a batch. See {@link #roundTrips}. */
  private int keysRead() {
    return gets.get() + batchedKeys.get();
  }

  void resetCounts() {
    observedKeys.clear();
    byThread.clear();
    gets.set(0);
    batchGets.set(0);
    prefixScans.set(0);
    prefixCounts.set(0);
    batchedKeys.set(0);
  }

  /** Renders this store's own section of the cost report. */
  void appendTo(StringBuilder out) {
    out.append("KV       roundTrips=")
        .append(roundTrips())
        .append("  keys=")
        .append(keysRead())
        .append("         gets=")
        .append(gets.get())
        .append("  batchGets=")
        .append(batchGets.get())
        .append("  prefixScans=")
        .append(prefixScans.get())
        .append("  prefixCounts=")
        .append(prefixCounts.get())
        .append('\n');
    synchronized (observedKeys) {
      if (!observedKeys.isEmpty()) {
        out.append("pointer keys read (").append(observedKeys.size()).append(")\n");
        observedKeys.forEach(k -> out.append("  ").append(k).append('\n'));
      }
    }
    byThread.appendTo(out, "  [kv] ");
  }
}
