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

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class ReconcileCancellationRegistry {
  private final ConcurrentMap<String, Thread> running = new ConcurrentHashMap<>();

  public void register(String jobId, Thread thread) {
    if (jobId == null || jobId.isBlank() || thread == null) {
      return;
    }
    running.put(jobId, thread);
  }

  public void unregister(String jobId, Thread thread) {
    if (jobId == null || jobId.isBlank() || thread == null) {
      return;
    }
    running.remove(jobId, thread);
  }

  public boolean requestCancel(String jobId) {
    if (jobId == null || jobId.isBlank()) {
      return false;
    }
    Thread thread = running.get(jobId);
    if (thread == null) {
      return false;
    }
    thread.interrupt();
    return true;
  }
}
