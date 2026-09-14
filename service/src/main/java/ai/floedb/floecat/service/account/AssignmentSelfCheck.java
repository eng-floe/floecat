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

/**
 * Background fence self-check for managed mode: batched consistent reads of the fence pointer of
 * each owned account every {@code floecat.account-assignment.self-check-interval}.
 */
@ApplicationScoped
public class AssignmentSelfCheck {

  @Inject AccountAssignment assignment;

  @Scheduled(
      every = "{floecat.account-assignment.self-check-interval}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = NotManaged.class)
  void sweep() {
    assignment.selfCheck();
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
