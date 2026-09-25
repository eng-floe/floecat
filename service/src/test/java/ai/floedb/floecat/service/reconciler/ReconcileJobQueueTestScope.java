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

package ai.floedb.floecat.service.reconciler;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobQueue;

/** Restores the process-wide queue setting after a serialized unit test. */
public final class ReconcileJobQueueTestScope implements AutoCloseable {
  public static final String LOCK_NAME = ReconcileJobQueue.ENABLED_PROPERTY;

  private final String previous;

  private ReconcileJobQueueTestScope(boolean enabled) {
    previous = System.getProperty(ReconcileJobQueue.ENABLED_PROPERTY);
    System.setProperty(ReconcileJobQueue.ENABLED_PROPERTY, Boolean.toString(enabled));
  }

  public static ReconcileJobQueueTestScope disabled() {
    return new ReconcileJobQueueTestScope(false);
  }

  @Override
  public void close() {
    if (previous == null) {
      System.clearProperty(ReconcileJobQueue.ENABLED_PROPERTY);
    } else {
      System.setProperty(ReconcileJobQueue.ENABLED_PROPERTY, previous);
    }
  }
}
