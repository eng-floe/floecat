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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class ReconcilerScheduler {
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;

  private final AtomicBoolean running = new AtomicBoolean(false);

  @Scheduled(
      every = "{reconciler.pollEvery:1s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void pollOnce() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    try {
      var lease = jobs.leaseNext().orElse(null);
      if (lease == null) {
        return;
      }

      jobs.markRunning(lease.jobId, System.currentTimeMillis());

      try {
        var connectorId =
            ResourceId.newBuilder()
                .setAccountId(lease.accountId)
                .setId(lease.connectorId)
                .setKind(ResourceKind.RK_CONNECTOR)
                .build();

        var principal =
            PrincipalContext.newBuilder()
                .setAccountId(lease.accountId)
                .setSubject("reconciler.scheduler")
                .setCorrelationId("reconciler-job-" + lease.jobId)
                .build();
        var result =
            reconcilerService.reconcile(
                principal,
                connectorId,
                lease.fullRescan,
                lease.scope,
                CaptureMode.METADATA_ONLY_CORE);
        if (!result.ok()) {
          jobs.markFailed(
              lease.jobId,
              System.currentTimeMillis(),
              result.message(),
              result.scanned,
              result.changed,
              result.errors);
          return;
        }
        var statsResult =
            reconcilerService.reconcile(
                principal,
                connectorId,
                lease.fullRescan,
                lease.scope,
                CaptureMode.STATS_ONLY_ASYNC);

        long finished = System.currentTimeMillis();
        if (statsResult.ok()) {
          jobs.markSucceeded(
              lease.jobId,
              finished,
              Math.max(result.scanned, statsResult.scanned),
              Math.max(result.changed, statsResult.changed));
        } else {
          jobs.markFailed(
              lease.jobId,
              finished,
              statsResult.message(),
              Math.max(result.scanned, statsResult.scanned),
              Math.max(result.changed, statsResult.changed),
              statsResult.errors);
        }
      } catch (Exception e) {
        var msg = describeFailure(e);
        jobs.markFailed(lease.jobId, System.currentTimeMillis(), msg, 0, 0, 1);
      }
    } finally {
      running.set(false);
    }
  }

  private static String describeFailure(Throwable t) {
    if (t == null) {
      return "Unknown error";
    }
    var seen = new java.util.HashSet<Throwable>();
    var parts = new java.util.ArrayList<String>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      seen.add(cur);
      parts.add(renderThrowable(cur));
      cur = cur.getCause();
    }
    return String.join(" | caused by: ", parts);
  }

  private static String renderThrowable(Throwable t) {
    if (t instanceof io.grpc.StatusRuntimeException sre) {
      var status = sre.getStatus();
      String desc = status.getDescription();
      if (desc == null || desc.isBlank()) {
        desc = sre.getMessage();
      }
      if (desc == null || desc.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return "grpc=" + status.getCode() + " desc=" + desc;
    }
    String msg = t.getMessage();
    String cls = t.getClass().getSimpleName();
    if (msg == null || msg.isBlank()) {
      return cls;
    }
    return cls + ": " + msg;
  }
}
