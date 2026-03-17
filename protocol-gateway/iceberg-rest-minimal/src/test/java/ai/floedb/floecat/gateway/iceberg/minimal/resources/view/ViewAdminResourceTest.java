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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.view;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import ai.floedb.floecat.gateway.iceberg.minimal.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.services.view.ViewBackend;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ViewAdminResourceTest {
  private final ViewBackend backend = Mockito.mock(ViewBackend.class);
  private final ViewAdminResource resource = new ViewAdminResource(backend);

  @Test
  void renameReturns204() {
    assertEquals(
        204,
        resource
            .rename(
                "foo",
                "idem-1",
                new RenameRequest(
                    new RenameRequest.TableIdentifierBody(List.of("db"), "orders_view"),
                    new RenameRequest.TableIdentifierBody(List.of("analytics"), "orders_view_new")))
            .getStatus());

    verify(backend)
        .rename("foo", List.of("db"), "orders_view", List.of("analytics"), "orders_view_new");
  }
}
