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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class StorageUsageMetrics {
  private static final String REBUILD_VERSION = "v1";

  @Inject AccountRepository accounts;
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject Observability observability;
  @Inject Tracer tracer;

  private final Map<String, AtomicLong> accountBytes = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> accountPointers = new ConcurrentHashMap<>();
  private StoreMetrics storeMetrics;
  private volatile String rebuildAccountToken = "";
  private volatile boolean rebuildComplete;

  @ConfigProperty(
      name = "floecat.metrics.storage.rebuild.max-objects-per-tick",
      defaultValue = "100")
  int rebuildMaxObjectsPerTick;

  @ConfigProperty(name = "floecat.metrics.storage.rebuild.max-tick-millis", defaultValue = "500")
  long rebuildMaxTickMillis;

  @ConfigProperty(name = "floecat.metrics.storage.rebuild.page-size", defaultValue = "100")
  int rebuildPageSize;

  @PostConstruct
  void init() {
    storeMetrics = new StoreMetrics(observability, "service", "storage.refresh");
  }

  @Scheduled(
      every = "${floecat.metrics.storage.refresh:30s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void refresh() {
    long startedNanos = System.nanoTime();
    Span refreshSpan =
        tracer
            .spanBuilder("service.storage.refresh")
            .setAttribute("floecat.component", "service")
            .setAttribute("floecat.operation", "storage.refresh")
            .startSpan();
    try (Scope ignored = refreshSpan.makeCurrent()) {
      ObservationScope refreshScope = storeMetrics.observe();
      boolean error = false;
      try {
        String token = "";
        StringBuilder next = new StringBuilder();
        do {
          var page = accounts.list(200, token, next);
          token = next.toString();
          next.setLength(0);
          for (var account : page) {
            String accountId = account.getResourceId().getId();
            try {
              var usage =
                  StorageAccountingPointerStore.decodeUsage(
                      pointerStore.get(Keys.accountStorageUsagePointer(accountId)).orElse(null));
              updateGauge(
                  accountPointers,
                  ServiceMetrics.Storage.ACCOUNT_POINTERS,
                  accountId,
                  usage.pointers());
              updateGauge(
                  accountBytes, ServiceMetrics.Storage.ACCOUNT_BYTES, accountId, usage.bytes());
              storeMetrics.recordBytes(usage.bytes(), "success", Tag.of(TagKey.ACCOUNT, accountId));
              storeMetrics.recordRequest("success", Tag.of(TagKey.ACCOUNT, accountId));
            } catch (RuntimeException e) {
              error = true;
              observability.counter(
                  ServiceMetrics.Storage.FAILURES,
                  1.0,
                  Tag.of(TagKey.OPERATION, "refresh"),
                  Tag.of(TagKey.ACCOUNT, accountId));
            }
          }
        } while (!token.isBlank());
      } finally {
        if (error) {
          refreshScope.error(new IllegalStateException("storage refresh encountered errors"));
        } else {
          refreshScope.success();
        }
        refreshScope.close();
      }
    } finally {
      refreshSpan.end();
      observability.timer(
          ServiceMetrics.Storage.REFRESH_DURATION,
          Duration.ofNanos(System.nanoTime() - startedNanos),
          Tag.of(TagKey.OPERATION, "refresh"));
    }
  }

  /**
   * Migrates legacy pointers in bounded slices. A durable completion marker makes this a one-time
   * rebuild; refresh never invokes it and never falls back to scans or object HEAD requests.
   */
  @Scheduled(
      every = "${floecat.metrics.storage.rebuild.tick-every:1s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void rebuildExistingUsage() {
    if (rebuildComplete) {
      return;
    }
    long startedNanos = System.nanoTime();
    try {
      if (pointerStore.get(Keys.storageUsageRebuildCompletePointer()).isPresent()) {
        rebuildComplete = true;
        return;
      }
      long deadline = System.currentTimeMillis() + Math.max(1L, Math.max(0L, rebuildMaxTickMillis));
      int remaining = Math.max(1, rebuildMaxObjectsPerTick);
      int sampled = 0;
      int failures = 0;
      StringBuilder nextAccounts = new StringBuilder();
      var page = accounts.list(200, rebuildAccountToken, nextAccounts);
      for (var account : page) {
        if (remaining <= 0 || System.currentTimeMillis() >= deadline) {
          break;
        }
        String accountId = account.getResourceId().getId();
        RebuildState state = loadRebuildState(accountId);
        if (state.complete()) {
          continue;
        }
        StringBuilder nextPointers = new StringBuilder();
        List<Pointer> pointers =
            pointerStore.listPointersByPrefix(
                Keys.accountRootPrefix(accountId),
                Math.min(Math.max(1, rebuildPageSize), remaining),
                state.pointerToken(),
                nextPointers);
        long pointersFound = state.pointers();
        long bytesFound = state.bytes();
        String lastVisited = state.pointerToken();
        boolean pageFailed = false;
        for (Pointer pointer : pointers) {
          if (remaining <= 0 || System.currentTimeMillis() >= deadline) {
            break;
          }
          remaining--;
          if (StorageAccountingPointerStore.usageKeyFor(pointer.getKey()) == null) {
            lastVisited = pointer.getKey();
            continue;
          }
          // Only legacy rows are absent from the incremental ledger.
          if (pointer.hasReferencedObjectSizeBytes()) {
            lastVisited = pointer.getKey();
            continue;
          }
          long objectBytes = 0L;
          if (PointerReferences.isBlobPointer(pointer) && !pointer.getBlobUri().isBlank()) {
            sampled++;
            try {
              objectBytes =
                  blobStore
                      .head(pointer.getBlobUri())
                      .map(header -> Math.max(0L, header.getContentLength()))
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "missing storage object " + pointer.getBlobUri()));
            } catch (RuntimeException e) {
              failures++;
              pageFailed = true;
              break;
            }
          }
          pointersFound++;
          bytesFound += objectBytes;
          lastVisited = pointer.getKey();
        }
        boolean consumedWholePage =
            !pageFailed
                && (pointers.size() == 0 || lastVisited.equals(pointers.getLast().getKey()));
        String nextToken =
            consumedWholePage
                ? nextPointers.toString()
                : lastVisited.equals(state.pointerToken())
                    ? state.pointerToken()
                    : pointerStore.pageTokenAfterKey(lastVisited);
        RebuildState nextState =
            new RebuildState(
                nextToken, pointersFound, bytesFound, !pageFailed && nextToken.isBlank());
        if (nextState.complete()) {
          completeRebuild(accountId, state, nextState);
        } else {
          saveRebuildState(accountId, state, nextState);
          break;
        }
      }
      if (sampled > 0) {
        observability.summary(ServiceMetrics.Storage.REBUILD_OBJECTS_SAMPLED, sampled);
      }
      if (failures > 0) {
        observability.counter(
            ServiceMetrics.Storage.FAILURES, failures, Tag.of(TagKey.OPERATION, "rebuild"));
      }
      if (remaining > 0 && !page.isEmpty() && nextAccounts.toString().isBlank()) {
        boolean allComplete =
            page.stream()
                .allMatch(account -> loadRebuildState(account.getResourceId().getId()).complete());
        if (allComplete) {
          String completionKey = Keys.storageUsageRebuildCompletePointer();
          rebuildComplete =
              pointerStore.compareAndSet(
                      completionKey,
                      0L,
                      PointerReferences.opaqueMarkerPointer(completionKey, REBUILD_VERSION, 1L))
                  || pointerStore.get(completionKey).isPresent();
        }
      } else if (!nextAccounts.toString().isBlank() && remaining > 0) {
        rebuildAccountToken = nextAccounts.toString();
      }
    } finally {
      observability.timer(
          ServiceMetrics.Storage.REBUILD_DURATION,
          Duration.ofNanos(System.nanoTime() - startedNanos),
          Tag.of(TagKey.OPERATION, "rebuild"));
    }
  }

  private RebuildState loadRebuildState(String accountId) {
    return decodeRebuildState(
        pointerStore.get(Keys.accountStorageUsageRebuildPointer(accountId)).orElse(null));
  }

  private void saveRebuildState(String accountId, RebuildState before, RebuildState after) {
    String key = Keys.accountStorageUsageRebuildPointer(accountId);
    Pointer current = pointerStore.get(key).orElse(null);
    long expected = current == null ? 0L : current.getVersion();
    if (!pointerStore.compareAndSet(
        key,
        expected,
        PointerReferences.opaqueMarkerPointer(key, encodeRebuildState(after), expected + 1L))) {
      throw new IllegalStateException("storage usage rebuild checkpoint conflict for " + accountId);
    }
  }

  private void completeRebuild(String accountId, RebuildState before, RebuildState completed) {
    String usageKey = Keys.accountStorageUsagePointer(accountId);
    String rebuildKey = Keys.accountStorageUsageRebuildPointer(accountId);
    Pointer usagePointer = pointerStore.get(usageKey).orElse(null);
    Pointer rebuildPointer = pointerStore.get(rebuildKey).orElse(null);
    var usage = StorageAccountingPointerStore.decodeUsage(usagePointer);
    var merged =
        new StorageAccountingPointerStore.AccountUsage(
            usage.pointers() + completed.pointers(), usage.bytes() + completed.bytes());
    long usageVersion = usagePointer == null ? 0L : usagePointer.getVersion();
    long rebuildVersion = rebuildPointer == null ? 0L : rebuildPointer.getVersion();
    if (!pointerStore.compareAndSetBatch(
        List.of(
            new PointerStore.CasUpsert(
                usageKey,
                usageVersion,
                PointerReferences.opaqueMarkerPointer(
                    usageKey,
                    StorageAccountingPointerStore.encodeUsage(merged),
                    usageVersion + 1L)),
            new PointerStore.CasUpsert(
                rebuildKey,
                rebuildVersion,
                PointerReferences.opaqueMarkerPointer(
                    rebuildKey, encodeRebuildState(completed), rebuildVersion + 1L))))) {
      throw new IllegalStateException("storage usage rebuild completion conflict for " + accountId);
    }
  }

  private void updateGauge(
      Map<String, AtomicLong> map, MetricId metric, String accountId, long value) {
    map.computeIfAbsent(
            accountId,
            tid -> {
              AtomicLong holder = new AtomicLong();
              observability.gauge(
                  metric, holder::get, "Storage account metric", Tag.of(TagKey.ACCOUNT, tid));
              return holder;
            })
        .set(value);
  }

  static RebuildState decodeRebuildState(Pointer pointer) {
    if (pointer == null || !PointerReferences.isOpaqueMarkerPointer(pointer)) {
      return new RebuildState("", 0L, 0L, false);
    }
    String[] fields = pointer.getBlobUri().split("\\n", -1);
    if (fields.length != 5 || !REBUILD_VERSION.equals(fields[0])) {
      return new RebuildState("", 0L, 0L, false);
    }
    try {
      return new RebuildState(
          fields[1],
          Long.parseLong(fields[2]),
          Long.parseLong(fields[3]),
          Boolean.parseBoolean(fields[4]));
    } catch (NumberFormatException ignored) {
      return new RebuildState("", 0L, 0L, false);
    }
  }

  static String encodeRebuildState(RebuildState state) {
    return REBUILD_VERSION
        + "\n"
        + state.pointerToken()
        + "\n"
        + state.pointers()
        + "\n"
        + state.bytes()
        + "\n"
        + state.complete();
  }

  record RebuildState(String pointerToken, long pointers, long bytes, boolean complete) {
    RebuildState {
      pointerToken = pointerToken == null ? "" : pointerToken;
      pointers = Math.max(0L, pointers);
      bytes = Math.max(0L, bytes);
    }
  }
}
