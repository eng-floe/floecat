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

package ai.floedb.floecat.delta.identity;

import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import java.util.Map;

/** Builds identity facts through the production current-schema resolution path. */
final class TestSnapshots {
  private TestSnapshots() {}

  static DeltaIdentitySnapshot identity(
      long version, StructType schema, Map<String, String> properties, Protocol protocol) {
    return KernelAdapter.currentSchemaIdentity(
        version,
        schema.toJson(),
        properties,
        protocol.getMinReaderVersion(),
        protocol.getMinWriterVersion(),
        protocol.getReaderFeatures(),
        protocol.getWriterFeatures());
  }
}
