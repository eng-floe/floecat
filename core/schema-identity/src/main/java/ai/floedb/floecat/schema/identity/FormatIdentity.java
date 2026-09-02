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

package ai.floedb.floecat.schema.identity;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import java.util.Objects;

/**
 * What the source format said about a node's identity.
 *
 * <p>Kept separate from the canonical Floecat id so the two never blur. They coincide whenever the
 * format supplied an id and policy adopted it, and they diverge wherever Floecat had to assign one
 * itself — which for Delta can happen within a single table, node by node.
 *
 * @param format the format that assigned {@code fieldId}
 * @param fieldId the format-assigned id, always positive
 */
public record FormatIdentity(TableFormat format, int fieldId) {

  public FormatIdentity {
    Objects.requireNonNull(format, "format");
    if (fieldId <= 0) {
      throw new IllegalArgumentException("A native field id must be positive, got " + fieldId);
    }
  }
}
