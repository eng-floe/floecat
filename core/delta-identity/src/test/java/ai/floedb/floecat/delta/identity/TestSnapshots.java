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
import java.util.Map;

/** Supplies the Kernel read facts used by adapter tests. */
final class TestSnapshots {
  private TestSnapshots() {}

  static Snapshot snapshot(
      long version, StructType schema, Map<String, String> properties, Protocol protocol) {
    Snapshot snapshot =
        org.mockito.Mockito.mock(
            Snapshot.class,
            org.mockito.Mockito.withSettings()
                .extraInterfaces(KernelAdapter.ProtocolSnapshot.class));
    org.mockito.Mockito.when(snapshot.getVersion()).thenReturn(version);
    org.mockito.Mockito.when(snapshot.getSchema()).thenReturn(schema);
    org.mockito.Mockito.when(snapshot.getTableProperties()).thenReturn(properties);
    org.mockito.Mockito.when(((KernelAdapter.ProtocolSnapshot) snapshot).protocol())
        .thenReturn(protocol);
    return snapshot;
  }
}
