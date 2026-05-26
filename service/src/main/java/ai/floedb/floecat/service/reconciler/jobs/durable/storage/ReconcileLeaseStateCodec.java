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

package ai.floedb.floecat.service.reconciler.jobs.durable.storage;

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class ReconcileLeaseStateCodec {
  private ReconcilePayloadStore payloadStore;

  public void bind(ReconcilePayloadStore payloadStore) {
    this.payloadStore = payloadStore;
  }

  public String encode(StoredJobLease lease) {
    return payloadStore.encodeInlineJobLease(lease);
  }

  public Optional<StoredJobLease> decode(String reference) {
    return payloadStore.readInlineJobLease(reference);
  }
}
