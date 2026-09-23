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

package ai.floedb.floecat.reconciler.jobs;

import org.eclipse.microprofile.config.ConfigProvider;

/** Process-wide admission switch for every reconcile job queue implementation. */
public final class ReconcileJobQueue {
  public static final String ENABLED_PROPERTY = "floecat.reconciler.job-queue.enabled";

  private ReconcileJobQueue() {}

  public static boolean isEnabled() {
    try {
      return ConfigProvider.getConfig()
          .getOptionalValue(ENABLED_PROPERTY, Boolean.class)
          .orElse(true);
    } catch (RuntimeException ignored) {
      // Plain unit construction can run without a MicroProfile Config provider.
      return Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"));
    }
  }

  public static void requireEnabled() {
    if (!isEnabled()) {
      throw new DisabledException();
    }
  }

  public static final class DisabledException extends IllegalStateException {
    public DisabledException() {
      super("Floecat reconcile job queue is disabled");
    }
  }
}
