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
package ai.floedb.floecat.catalog.sharing;

import ai.floedb.floecat.client.sharing.DeltaSharingModel.Protocol;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableMetadata;
import ai.floedb.floecat.delta.identity.DeltaHistorySource.HistoricalMetadata;
import ai.floedb.floecat.delta.identity.DeltaHistorySource.HistoricalProtocol;
import ai.floedb.floecat.delta.identity.DeltaIdentitySnapshot;
import ai.floedb.floecat.delta.identity.KernelAdapter;
import java.util.Set;

/** Converts Delta Sharing protocol actions into the canonical identity model. */
final class DeltaSharingIdentityActions {
  private DeltaSharingIdentityActions() {}

  static DeltaIdentitySnapshot snapshot(
      long version, TableMetadata metadata, Protocol protocol) {
    HistoricalMetadata historicalMetadata = metadata(metadata);
    HistoricalProtocol historicalProtocol = protocol(protocol);
    return KernelAdapter.currentSchemaIdentity(
        version,
        historicalMetadata.schemaJson(),
        historicalMetadata.configuration(),
        historicalProtocol.minReaderVersion(),
        historicalProtocol.minWriterVersion(),
        historicalProtocol.readerFeatures(),
        historicalProtocol.writerFeatures());
  }

  static HistoricalMetadata metadata(TableMetadata metadata) {
    return new HistoricalMetadata(metadata.schemaJson(), metadata.configuration());
  }

  static HistoricalProtocol protocol(Protocol protocol) {
    return new HistoricalProtocol(
        protocol.minReaderVersion(),
        protocol.minWriterVersion(),
        Set.copyOf(protocol.readerFeatures()),
        Set.copyOf(protocol.writerFeatures()));
  }
}
