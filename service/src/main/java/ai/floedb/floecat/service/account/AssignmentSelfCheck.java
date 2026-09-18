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

package ai.floedb.floecat.service.account;

import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.ScheduledExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import org.jboss.logging.Logger;

/**
 * The managed-mode background sweep: a batched consistent read of the fence pointer of each owned
 * account every {@code floecat.account-assignment.self-check-interval}, and a warning while no
 * control plane has reached this process.
 */
@ApplicationScoped
public class AssignmentSelfCheck {
  private static final Logger LOG = Logger.getLogger(AssignmentSelfCheck.class);
  private static final long UNASSIGNED_WARN_NANOS = Duration.ofMinutes(1).toNanos();

  @Inject AccountAssignment assignment;

  private long lastUnassignedWarnNanos = System.nanoTime();

  @Scheduled(
      every = "{floecat.account-assignment.self-check-interval}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = NotManaged.class)
  void sweep() {
    assignment.selfCheck();
    warnIfNoControlPlane();
  }

  /**
   * A managed process nobody assigns refuses account-scoped writes with a message naming the
   * account, which reads as a routing problem. It cannot tell a control plane that has not spoken
   * yet from one that does not exist, so it warns rather than fails, at most once a minute whatever
   * the sweep cadence, and not before the first minute -- a pod that starts ahead of its control
   * plane is ordinary.
   */
  private void warnIfNoControlPlane() {
    if (assignment.everAssigned()
        || System.nanoTime() - lastUnassignedWarnNanos < UNASSIGNED_WARN_NANOS) {
      return;
    }
    lastUnassignedWarnNanos = System.nanoTime();
    LOG.warnf(
        "account_assignment_absent member=%s incarnation=%s: managed mode has accepted no"
            + " assignment, so it owns no account and refuses account-scoped writes. Either nothing"
            + " is calling ApplyAssignment, or what it sends is being rejected -- the caller sees"
            + " the reason. See docs/service.md#binding-a-control-plane.",
        assignment.memberId(), assignment.incarnation());
  }

  @Singleton
  public static class NotManaged implements Scheduled.SkipPredicate {
    @Inject AccountAssignment assignment;

    @Override
    public boolean test(ScheduledExecution execution) {
      return !assignment.managed();
    }
  }
}
