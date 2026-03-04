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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class TableCommitOutboxScheduler {

  @Inject TableCommitOutboxService outboxService;
  @Inject TableGatewaySupport tableSupport;

  @Scheduled(
      every = "${floecat.gateway.table-commit-outbox.tick-every:30s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    boolean enabled =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.enabled", Boolean.class)
            .orElse(true);
    if (!enabled) {
      return;
    }
    int batchSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.batch-size", Integer.class)
            .orElse(100);
    outboxService.drainPending(tableSupport, batchSize);
  }
}
