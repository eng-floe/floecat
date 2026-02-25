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

package ai.floedb.floecat.service.query.resolver;

import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.TableObligations;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/**
 * Resolves governance obligations (masks, row filters). For now, DescribeInputs does not return
 * obligations, but they are stored in the QueryContext for planner use.
 */
@ApplicationScoped
public class ObligationsResolver {

  /**
   * Result includes both the decoded obligations and the exact bytes stored on the {@link
   * QueryContext}.
   */
  public record Result(List<TableObligations> obligations, byte[] bytes) {}

  public Result resolveObligations(String correlationId, List<SnapshotPin> pins) {
    // TODO: load obligations from governance service
    return new Result(List.of(), new byte[0]);
  }
}
