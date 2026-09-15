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

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Restores this member's accounts from {@code assignments/<member>} at startup, before its control
 * plane has said anything. Synchronous in the startup observer so requests never see a
 * half-recovered set.
 */
@ApplicationScoped
public class AssignmentRecovery {
  private static final Logger LOG = Logger.getLogger(AssignmentRecovery.class);

  @Inject AccountAssignment assignment;

  void onStart(@Observes @Priority(10) StartupEvent startup) {
    if (!assignment.managed()) {
      return;
    }
    AssignmentControl.Status status = assignment.recoverFromStore();
    LOG.infof(
        "account_assignment_startup member=%s incarnation=%s recovered=%s accounts=%d",
        assignment.memberId(),
        assignment.incarnation(),
        status.recoveredFromStore(),
        status.accounts().size());
  }
}
