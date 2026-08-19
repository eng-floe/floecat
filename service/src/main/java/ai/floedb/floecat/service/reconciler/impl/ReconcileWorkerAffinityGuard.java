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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileWorkerAffinity;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Enforces the remote worker, serving deployment, and leased job affinity contract. */
@ApplicationScoped
public class ReconcileWorkerAffinityGuard {
  @Inject ReconcileJobStore jobs;

  @ConfigProperty(name = "floecat.reconciler.worker-affinity", defaultValue = "reconciler-v1")
  String configuredAffinity = "reconciler-v1";

  public void requireLeaseRequestAffinity(String requestedAffinity) {
    ReconcileWorkerAffinity requested = ReconcileWorkerAffinity.of(requestedAffinity);
    ReconcileWorkerAffinity configured = configuredAffinity();
    if (!requested.equals(configured)) {
      throw mismatch(
          "reconcile worker affinity does not match this deployment", requested, configured);
    }
  }

  /** Rejects a lease-bound call routed through a deployment that does not own the job's cohort. */
  public void requireJobAffinity(String jobId) {
    if (jobId == null || jobId.isBlank()) {
      return;
    }
    jobs.getCompactLeaseView(jobId)
        .ifPresent(
            job -> {
              ReconcileWorkerAffinity jobAffinity =
                  ReconcileWorkerAffinity.fromPolicy(job.executionPolicy);
              ReconcileWorkerAffinity configured = configuredAffinity();
              if (!jobAffinity.equals(configured)) {
                throw mismatch(
                    "reconcile job affinity does not match this deployment",
                    jobAffinity,
                    configured);
              }
            });
  }

  ReconcileWorkerAffinity configuredAffinity() {
    return ReconcileWorkerAffinity.of(configuredAffinity);
  }

  private static io.grpc.StatusRuntimeException mismatch(
      String message, ReconcileWorkerAffinity actual, ReconcileWorkerAffinity expected) {
    return Status.FAILED_PRECONDITION
        .withDescription(
            message + ": worker_or_job=" + display(actual) + " deployment=" + display(expected))
        .asRuntimeException();
  }

  private static String display(ReconcileWorkerAffinity affinity) {
    return affinity == null || !affinity.enabled() ? "<unversioned>" : affinity.value();
  }
}
