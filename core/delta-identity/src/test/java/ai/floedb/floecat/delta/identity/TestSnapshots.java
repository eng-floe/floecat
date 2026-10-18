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

import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;

/** Builds protocol-bearing snapshots through the production current-schema path. */
final class TestSnapshots {
  private TestSnapshots() {}

  static Snapshot snapshot(
      long version, StructType schema, Map<String, String> properties, Protocol protocol) {
    return KernelAdapter.currentSchemaSnapshot(
        "s3://bucket/table",
        version,
        schema.toJson(),
        properties,
        List.of(),
        protocol.getMinReaderVersion(),
        protocol.getMinWriterVersion(),
        protocol.getReaderFeatures(),
        protocol.getWriterFeatures());
  }
}
