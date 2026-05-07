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

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CredentialsResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableRef;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.planning.PlanTaskManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TableCredentialService {
  @Inject PlanTaskManager planTaskManager;

  public Response load(TableRef tableContext, String planId) {
    if (planId == null || planId.isBlank()) {
      return IcebergErrorResponses.validation("planId is required");
    }
    var plan = planTaskManager.findPlan(planId.trim(), tableContext.tableId());
    if (plan.isEmpty()) {
      return IcebergErrorResponses.noSuchPlanId("plan " + planId.trim() + " not found");
    }
    PlanTaskManager.PlanDescriptor descriptor = plan.get();
    if (descriptor.status() != PlanTaskManager.PlanStatus.COMPLETED) {
      return IcebergErrorResponses.noSuchPlanId("plan " + planId.trim() + " not available");
    }
    List<StorageCredentialDto> plannedCredentials = descriptor.credentials();
    if (plannedCredentials == null || plannedCredentials.isEmpty()) {
      return IcebergErrorResponses.noSuchPlanId("plan " + planId.trim() + " not available");
    }
    return Response.ok(new CredentialsResponseDto(plannedCredentials)).build();
  }
}
