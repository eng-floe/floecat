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

package ai.floedb.floecat.telemetry.profiling;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.opentelemetry.api.trace.Span;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import jdk.jfr.Recording;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@ApplicationScoped
public class ProfilingCaptureService {
  private static final Logger LOG = LoggerFactory.getLogger(ProfilingCaptureService.class);

  private final ProfilingConfig config;
  private final Observability observability;
  private final CaptureIndex captureIndex;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread thread = new Thread(r, "profiling-capture");
            thread.setDaemon(true);
            return thread;
          });
  private final AtomicBoolean capturing = new AtomicBoolean();
  private final Deque<Instant> rateWindow = new ArrayDeque<>();

  public ProfilingCaptureService(
      ProfilingConfig config, Observability observability, CaptureIndex captureIndex) {
    this.config = Objects.requireNonNull(config, "config");
    this.observability = Objects.requireNonNull(observability, "observability");
    this.captureIndex = Objects.requireNonNull(captureIndex, "captureIndex");
  }

  public CaptureMetadata startCapture(
      String trigger, Duration requestedDuration, String mode, String scope, String requestedBy) {
    if (!config.enabled()) {
      throw new ProfilingException("profiling disabled", ProfilingReason.DISABLED);
    }
    Duration duration = requestedDuration == null ? config.captureDuration() : requestedDuration;
    try {
      captureIndex.pruneTo(config.totalMaxBytes());
    } catch (IOException e) {
      LOG.warn("failed to prune profiling artifacts", e);
    }
    if (!allowRate()) {
      recordMetric(trigger, mode, scope, "dropped", ProfilingReason.RATE_LIMIT);
      throw new ProfilingException("rate limit exceeded", ProfilingReason.RATE_LIMIT);
    }
    if (!capturing.compareAndSet(false, true)) {
      recordMetric(trigger, mode, scope, "dropped", ProfilingReason.ALREADY_RUNNING);
      throw new ProfilingException("capture already running", ProfilingReason.ALREADY_RUNNING);
    }

    if (captureIndex.totalBytes() + config.maxCaptureBytes() > config.totalMaxBytes()) {
      capturing.set(false);
      recordMetric(trigger, mode, scope, "dropped", ProfilingReason.DISK_CAP);
      throw new ProfilingException("disk cap exceeded", ProfilingReason.DISK_CAP);
    }

    String id = UUID.randomUUID().toString();
    Path artifact = captureIndex.artifactFor(id);
    CaptureMetadata meta = new CaptureMetadata(id, trigger, mode, scope, "started");
    meta.setRequestedBy(requestedBy);
    meta.setRequestedDurationMs(duration.toMillis());
    var spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      meta.setTraceId(spanContext.getTraceId());
      meta.setSpanId(spanContext.getSpanId());
    }
    meta.setArtifactPath(artifact.toString());
    captureIndex.persist(meta);
    recordMetric(trigger, mode, scope, "started", null);
    logCaptureStart(meta, duration);

    try {
      Recording recording = new Recording();
      recording.setName("profiling-" + id);
      recording.setDestination(artifact);
      recording.start();
      scheduler.schedule(
          () -> finishCapture(id, recording, artifact), duration.toMillis(), TimeUnit.MILLISECONDS);
    } catch (IOException e) {
      capturing.set(false);
      recordMetric(trigger, mode, scope, "failed", ProfilingReason.IO_ERROR);
      throw new RuntimeException("failed to start profiling capture", e);
    }
    return meta;
  }

  private void finishCapture(String id, Recording recording, Path artifact) {
    try {
      recording.stop();
      recording.close();
      long size = Files.exists(artifact) ? Files.size(artifact) : 0;
      CaptureMetadata meta = captureIndex.find(id).orElseThrow();
      meta.setEndTime(Instant.now());
      meta.setDurationMs(Duration.between(meta.getStartTime(), meta.getEndTime()).toMillis());
      meta.setArtifactSizeBytes(size);
      meta.setResult("completed");
      captureIndex.persist(meta);
      recordMetric(meta.getTrigger(), meta.getMode(), meta.getScope(), "completed", null);
      logCaptureComplete(meta, size);
    } catch (Exception e) {
      CaptureMetadata meta = captureIndex.find(id).orElse(null);
      if (meta != null) {
        meta.setResult("failed");
        meta.setReason(ProfilingReason.IO_ERROR.tagValue());
        meta.setEndTime(Instant.now());
        captureIndex.persist(meta);
        recordMetric(
            meta.getTrigger(), meta.getMode(), meta.getScope(), "failed", ProfilingReason.IO_ERROR);
        logCaptureFailure(meta, e);
      }
    } finally {
      capturing.set(false);
    }
  }

  private boolean allowRate() {
    Instant now = Instant.now();
    Instant cutoff = now.minus(config.rateWindow());
    while (!rateWindow.isEmpty() && rateWindow.peekFirst().isBefore(cutoff)) {
      rateWindow.removeFirst();
    }
    if (rateWindow.size() >= config.rateLimit()) {
      return false;
    }
    rateWindow.addLast(now);
    return true;
  }

  private void recordMetric(
      String trigger, String mode, String scope, String result, ProfilingReason reason) {
    var tags = new ArrayDeque<Tag>();
    tags.add(Tag.of(TagKey.COMPONENT, "service"));
    tags.add(Tag.of(TagKey.OPERATION, "profiling"));
    tags.add(Tag.of(TagKey.TRIGGER, trigger == null || trigger.isBlank() ? "manual" : trigger));
    tags.add(Tag.of(TagKey.SCOPE, scope == null ? "manual" : scope));
    tags.add(Tag.of(TagKey.MODE, mode == null ? "jfr" : mode));
    tags.add(Tag.of(TagKey.RESULT, result));
    if (reason != null) {
      tags.add(Tag.of(TagKey.REASON, reason.tagValue()));
    }
    observability.counter(ProfilingMetrics.Captures.TOTAL, 1d, tags.toArray(new Tag[0]));
  }

  public Optional<CaptureMetadata> latest() {
    return captureIndex.latest();
  }

  public Optional<CaptureMetadata> find(String id) {
    return captureIndex.find(id);
  }

  Path artifactPath(String id) {
    return captureIndex.artifactFor(id);
  }

  private void logCaptureStart(CaptureMetadata meta, Duration duration) {
    try (AutoCloseable scope = withMdc(meta)) {
      LOG.info(
          "Profiling capture {} started (duration={}ms) trigger={} mode={} scope={} traceId={}",
          meta.getId(),
          duration.toMillis(),
          meta.getTrigger(),
          meta.getMode(),
          meta.getScope(),
          meta.getTraceId());
    } catch (Exception ignored) {
    }
  }

  private void logCaptureComplete(CaptureMetadata meta, long size) {
    try (AutoCloseable scope = withMdc(meta)) {
      LOG.info(
          "Profiling capture {} completed (size={} bytes) trigger={} mode={} scope={} traceId={}",
          meta.getId(),
          size,
          meta.getTrigger(),
          meta.getMode(),
          meta.getScope(),
          meta.getTraceId());
    } catch (Exception ignored) {
    }
  }

  private void logCaptureFailure(CaptureMetadata meta, Exception e) {
    try (AutoCloseable scope = withMdc(meta)) {
      LOG.error(
          "Profiling capture {} failed (reason={}) traceId={}",
          meta.getId(),
          meta.getReason(),
          meta.getTraceId(),
          e);
    } catch (Exception inner) {
      LOG.error("Profiling capture logging failed", inner);
    }
  }

  private AutoCloseable withMdc(CaptureMetadata meta) {
    MDC.put("captureId", meta.getId());
    MDC.put("captureTrigger", meta.getTrigger());
    MDC.put("captureMode", meta.getMode());
    MDC.put("captureScope", meta.getScope());
    return () -> {
      MDC.remove("captureId");
      MDC.remove("captureTrigger");
      MDC.remove("captureMode");
      MDC.remove("captureScope");
    };
  }

  @PreDestroy
  void shutdown() {
    scheduler.shutdownNow();
  }
}
