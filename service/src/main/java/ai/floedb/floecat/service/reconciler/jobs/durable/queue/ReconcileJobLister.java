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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJobPage;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class ReconcileJobLister {
  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileJobProjector projector;

  public void bind(ReconcileJobIndexStore jobIndexStore, ReconcileJobProjector projector) {
    this.jobIndexStore = jobIndexStore;
    this.projector = projector;
  }

  public ReconcileJobPage list(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    var page = jobIndexStore.listStoredJobs(accountId, pageSize, pageToken, connectorId, states);
    return new ReconcileJobPage(
        page.records().stream().map(projector::toPublicJobSummary).toList(), page.nextPageToken());
  }

  public ReconcileJobPage childJobsPage(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    var page = jobIndexStore.listStoredChildJobs(accountId, parentJobId, pageSize, pageToken);
    List<ReconcileJob> out = new ArrayList<>(page.records().size());
    for (StoredReconcileJob stored : page.records()) {
      out.add(projector.toPublicJob(stored, true));
    }
    return new ReconcileJobPage(out, page.nextPageToken());
  }
}
