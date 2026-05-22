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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import java.util.List;

public class StoredFileGroupPlanPayload {
  public int fileCount;
  public List<String> filePaths = List.of();

  public static StoredFileGroupPlanPayload of(ReconcileFileGroupTask task) {
    ReconcileFileGroupTask effective = task == null ? ReconcileFileGroupTask.empty() : task;
    StoredFileGroupPlanPayload payload = new StoredFileGroupPlanPayload();
    payload.fileCount = effective.fileCount();
    payload.filePaths = effective.filePaths() == null ? List.of() : effective.filePaths();
    return payload;
  }

  public int fileCount() {
    if (fileCount > 0) {
      return fileCount;
    }
    return filePaths == null ? 0 : filePaths.size();
  }

  public List<String> filePaths() {
    return filePaths == null ? List.of() : filePaths;
  }
}
