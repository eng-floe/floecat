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
package ai.floedb.floecat.telemetry.jvm;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@ApplicationScoped
public class JvmRuntimeTelemetry {
  private static final String COMPONENT = "service";
  private static final String OPERATION = "jvm.runtime";
  private static final String DESCRIPTION_CPU = "Process CPU usage (ratio 0-1).";
  private static final String DESCRIPTION_THREADS = "Live thread count.";
  private static final String DESCRIPTION_MEMORY = "Used bytes for a JVM memory resource.";
  private static final String DESCRIPTION_GC_LIVE = "Estimated live data (bytes) held by the GC.";
  private static final String DESCRIPTION_GC_RATE =
      "Live data growth rate (bytes/second) across GC-managed memory pools.";

  private final Observability observability;
  private final com.sun.management.OperatingSystemMXBean osBean;
  private final ThreadMXBean threadBean;
  private final MemoryMXBean memoryBean;
  private final Map<String, MemoryPoolMXBean> memoryPools;
  private final List<GarbageCollectorMXBean> gcBeans;
  private final Map<String, AtomicReference<Double>> gcAllocationRates = new ConcurrentHashMap<>();
  private final Map<String, Long> gcLastUsage = new ConcurrentHashMap<>();
  private volatile long lastSampleNanos = System.nanoTime();

  @Inject
  public JvmRuntimeTelemetry(Observability observability) {
    this.observability = observability;
    java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    this.osBean =
        os instanceof com.sun.management.OperatingSystemMXBean
            ? (com.sun.management.OperatingSystemMXBean) os
            : null;
    this.threadBean = ManagementFactory.getThreadMXBean();
    this.memoryBean = ManagementFactory.getMemoryMXBean();
    this.memoryPools =
        ManagementFactory.getMemoryPoolMXBeans().stream()
            .collect(
                Collectors.toMap(
                    MemoryPoolMXBean::getName, pool -> pool, (first, second) -> first));
    this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
  }

  @PostConstruct
  void registerGauges() {
    observability.gauge(
        JvmMetrics.PROCESS_CPU_USAGE, this::cpuUsage, DESCRIPTION_CPU, runtimeTags());

    observability.gauge(
        JvmMetrics.THREAD_COUNT,
        () -> (double) threadBean.getThreadCount(),
        DESCRIPTION_THREADS,
        runtimeTags());

    registerMemoryGauge("heap", () -> (double) memoryBean.getHeapMemoryUsage().getUsed());
    registerMemoryGauge("nonheap", () -> (double) memoryBean.getNonHeapMemoryUsage().getUsed());
    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
        .filter(pool -> pool.getName().equalsIgnoreCase("direct"))
        .findFirst()
        .ifPresent(pool -> registerMemoryGauge("direct", () -> (double) safeBufferUsage(pool)));

    for (GarbageCollectorMXBean gc : gcBeans) {
      gcAllocationRates.put(gc.getName(), new AtomicReference<>(0d));
      gcLastUsage.put(gc.getName(), liveDataForGc(gc));
      registerGcGauges(gc);
    }
  }

  @Scheduled(every = "10s")
  void sampleGcAllocationRates() {
    long now = System.nanoTime();
    double elapsedSeconds = Duration.ofNanos(now - lastSampleNanos).toMillis() / 1000d;
    if (elapsedSeconds <= 0) {
      lastSampleNanos = now;
      return;
    }
    for (GarbageCollectorMXBean gc : gcBeans) {
      String name = gc.getName();
      long current = liveDataForGc(gc);
      long previous = gcLastUsage.getOrDefault(name, current);
      double rate = Math.max(0, (current - previous) / elapsedSeconds);
      gcAllocationRates.get(name).set(rate);
      gcLastUsage.put(name, current);
    }
    lastSampleNanos = now;
  }

  private double cpuUsage() {
    if (osBean == null) {
      return 0d;
    }
    double load = osBean.getProcessCpuLoad();
    if (Double.isNaN(load) || load < 0) {
      return 0d;
    }
    return Math.min(1d, Math.max(0d, load));
  }

  private void registerMemoryGauge(String resource, Supplier<Number> supplier) {
    observability.gauge(
        JvmMetrics.MEMORY_USED,
        supplier,
        DESCRIPTION_MEMORY,
        runtimeTags(Tag.of(TagKey.RESOURCE, resource)));
  }

  private void registerGcGauges(GarbageCollectorMXBean gc) {
    Tag gcTag = Tag.of(TagKey.GC_NAME, gc.getName());
    observability.gauge(
        JvmMetrics.GC_LIVE_DATA_BYTES,
        () -> (double) liveDataForGc(gc),
        DESCRIPTION_GC_LIVE,
        runtimeTags(gcTag));
    observability.gauge(
        JvmMetrics.GC_LIVE_DATA_GROWTH_RATE,
        gcAllocationRates.get(gc.getName())::get,
        DESCRIPTION_GC_RATE,
        runtimeTags(gcTag));
  }

  private long liveDataForGc(GarbageCollectorMXBean gc) {
    long total = 0;
    for (String poolName : gc.getMemoryPoolNames()) {
      MemoryPoolMXBean pool = memoryPools.get(poolName);
      if (pool == null) {
        continue;
      }
      if (pool.getUsage() != null) {
        total += pool.getUsage().getUsed();
      }
    }
    return total;
  }

  private static long safeBufferUsage(BufferPoolMXBean pool) {
    if (pool == null) {
      return 0;
    }
    return pool.getMemoryUsed();
  }

  private Tag[] runtimeTags(Tag... extra) {
    Tag[] base =
        new Tag[] {Tag.of(TagKey.COMPONENT, COMPONENT), Tag.of(TagKey.OPERATION, OPERATION)};
    if (extra == null || extra.length == 0) {
      return base;
    }
    Tag[] combined = new Tag[base.length + extra.length];
    System.arraycopy(base, 0, combined, 0, base.length);
    System.arraycopy(extra, 0, combined, base.length, extra.length);
    return combined;
  }
}
