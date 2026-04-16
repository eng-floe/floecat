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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Default executor that preserves the existing in-process ReconcilerService execution path. */
@ApplicationScoped
public class DefaultReconcileExecutor implements ReconcileExecutor {
  private final ReconcilerService reconcilerService;
  private final boolean enabled;

  @Inject
  public DefaultReconcileExecutor(
      ReconcilerService reconcilerService,
      @ConfigProperty(name = "floecat.reconciler.executor.default.enabled", defaultValue = "true")
          boolean enabled) {
    this.reconcilerService = reconcilerService;
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "default_reconciler";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.EXEC_TABLE, ReconcileJobKind.EXEC_VIEW);
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    var lease = context.lease();
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

    ReconcilerService.Result result =
        lease.jobKind == ReconcileJobKind.EXEC_VIEW
            ? reconcilerService.reconcileView(
                principal,
                connectorId,
                lease.scope,
                lease.viewTask,
                null,
                context.shouldStop(),
                context.progressListener()::onProgress)
            : reconcilerService.reconcile(
                principal,
                connectorId,
                lease.fullRescan,
                lease.scope,
                lease.tableTask,
                lease.captureMode,
                null,
                context.shouldStop(),
                context.progressListener()::onProgress);

    if (result.cancelled()) {
      return ExecutionResult.cancelled(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message());
    }
    if (!result.ok()) {
      return ExecutionResult.failure(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          failureKindOf(result.error),
          result.message(),
          result.error);
    }
    return ExecutionResult.success(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        result.viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        result.message());
  }

  private static ExecutionResult.FailureKind failureKindOf(Exception error) {
    if (error instanceof ReconcileFailureException failure) {
      return failure.failureKind();
    }
    return ExecutionResult.FailureKind.INTERNAL;
  }
}
