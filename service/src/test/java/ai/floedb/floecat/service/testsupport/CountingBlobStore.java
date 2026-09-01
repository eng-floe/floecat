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

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.spi.BlobStore.Page;
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
public class CountingBlobStore extends InMemoryBlobStore {

  /** What kind of call fetched something. A LIST is not an object read; it is a walk. */
  enum Kind {
    GET,
    HEAD,
    LIST
  }

  /**
   * One fetch: what was read, how, and who asked.
   *
   * <p>One record rather than parallel lists of uris and stack traces. The two used to be appended
   * separately from the same call sites, so every uri was recorded twice and the report printed it
   * twice -- once in the summary and again at the head of its own origin trace.
   */
  record Fetch(Kind kind, String target, String frames) {}

  private final List<Fetch> fetches = Collections.synchronizedList(new ArrayList<>());

  private final AtomicInteger gets = new AtomicInteger();
  private final AtomicInteger batches = new AtomicInteger();

  /**
   * Objects pulled by a batch, counted individually. Kept out of {@code gets}, which counts calls.
   */
  private final AtomicInteger batchedObjects = new AtomicInteger();

  /** The same fetches, attributed to the thread that made them. */
  private final ByThread byThread = new ByThread();

  /**
   * HEADs are counted apart from GETs because they are the one store call a warm read is allowed to
   * make: pin validation always checks the live store, since the CAS GC's min-age fence measures
   * age since the blob was WRITTEN, so a cached observation is not an existence proof. A cost that
   * lumped the two together could only ever assert zero, which is wrong.
   */
  private final AtomicInteger heads = new AtomicInteger();

  /**
   * Object listings, counted because they are the operation the design forbids on a warm path.
   *
   * <p>"Nothing walks the store to find out what exists" is the guarantee; a LIST is exactly the
   * walk, and it is latency-bearing against S3. Uncounted, the one call that would break the
   * guarantee was the one the harness could not see.
   */
  private final AtomicInteger lists = new AtomicInteger();

  @Override
  public Page list(String prefix, int limit, String pageToken) {
    lists.incrementAndGet();
    byThread.record();
    fetches.add(new Fetch(Kind.LIST, prefix, origin()));
    return super.list(prefix, limit, pageToken);
  }

  @Override
  public Page listPrefixes(String prefix, int limit, String pageToken) {
    lists.incrementAndGet();
    byThread.record();
    fetches.add(new Fetch(Kind.LIST, prefix, origin()));
    return super.listPrefixes(prefix, limit, pageToken);
  }

  @Override
  public Optional<BlobHeader> head(String uri) {
    fetches.add(new Fetch(Kind.HEAD, uri, origin()));
    byThread.record();
    heads.incrementAndGet();
    return super.head(uri);
  }

  @Override
  public byte[] get(String uri) {
    gets.incrementAndGet();
    byThread.record();
    fetches.add(new Fetch(Kind.GET, uri, origin()));
    return super.get(uri);
  }

  @Override
  public Map<String, byte[]> getBatch(List<String> uris) {
    if (uris != null && !uris.isEmpty()) {
      batches.incrementAndGet();
      byThread.record();
      batchedObjects.addAndGet(uris.size());
      String origin = origin();
      uris.forEach(u -> fetches.add(new Fetch(Kind.GET, u, origin)));
    }
    return super.getBatch(uris);
  }

  /**
   * Objects this store served through GETs, counting each object of a batch. HEADs are not here --
   * see {@link #heads} -- and neither are listings; see {@link #listCalls}.
   *
   * <p>GETs and HEADs stay apart because the two coefficients do: a warm request is allowed HEADs
   * and is not allowed GETs, and one total covering both lets a GET appear as a HEAD disappearing.
   * A getBatch of eight uris is eight here, where the pointer side counts one round trip for the
   * same shape. See {@link CountingPointerStore#roundTrips} for why the two are not
   * interchangeable.
   */
  public int objectGets() {
    return gets.get() + batchedObjects.get();
  }

  /**
   * Round trips this store served, the unit the pointer side reports.
   *
   * <p>A getBatch of eight objects is one of these and eight of {@link #objectGets}. Keeping the
   * raw counter raw is what lets both be answered; folding fan-out into it would make a read path
   * refactored from eight gets into one batch look unchanged here while the pointer side moved by
   * seven.
   */
  public int roundTrips() {
    return gets.get() + batches.get() + heads.get() + lists.get();
  }

  /** HEADs alone. A warm read is allowed these; it is not allowed GETs. */
  public int heads() {
    return heads.get();
  }

  /**
   * Listing CALLS, kept out of {@link #objectGets}.
   *
   * <p>One list that returns a thousand keys is one call and a thousand keys, so adding it to an
   * object count would report "2 objects" for a read that pays for a thousand. It is counted and
   * reported on its own line, and a warm request asserts there are none.
   */
  public int listCalls() {
    return lists.get();
  }

  void resetCounts() {
    byThread.clear();
    fetches.clear();
    heads.set(0);
    gets.set(0);
    batches.set(0);
    batchedObjects.set(0);
    lists.set(0);
  }

  /** Stack frames kept per fetch: enough to name the repository and its caller, not a dump. */
  private static final int ORIGIN_FRAMES = 14;

  private static String origin() {
    StringBuilder trace = new StringBuilder();
    StackWalker.getInstance()
        .walk(
            frames ->
                frames
                    .filter(frame -> frame.getClassName().startsWith("ai.floedb.floecat"))
                    .filter(
                        frame ->
                            !frame.getClassName().startsWith(CountingBlobStore.class.getName()))
                    .limit(ORIGIN_FRAMES)
                    .toList())
        .forEach(frame -> trace.append("      at ").append(frame).append('\n'));
    return trace.toString();
  }

  /** Renders this store's own section of the cost report. */
  void appendTo(StringBuilder out) {
    out.append("S3       objectGets=")
        .append(objectGets())
        .append("  roundTrips=")
        .append(roundTrips())
        .append("  gets=")
        .append(gets.get())
        .append("  batches=")
        .append(batches.get())
        .append("  heads=")
        .append(heads.get())
        .append("  listCalls=")
        .append(listCalls())
        .append('\n');
    synchronized (fetches) {
      if (!fetches.isEmpty()) {
        out.append("blob fetches (").append(fetches.size()).append(")\n");
        fetches.forEach(
            f ->
                out.append("  ")
                    .append(f.kind())
                    .append(' ')
                    .append(f.target())
                    .append('\n')
                    .append(f.frames()));
      }
    }
    byThread.appendTo(out, "  [s3] ");
  }
}
