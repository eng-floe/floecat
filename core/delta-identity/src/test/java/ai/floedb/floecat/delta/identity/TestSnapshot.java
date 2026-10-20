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

import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.statistics.SnapshotStatistics;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A protocol-bearing Kernel snapshot built directly from a schema, for identity tests. */
record TestSnapshot(
    long version, StructType schema, Map<String, String> properties, Protocol protocol)
    implements Snapshot, KernelAdapter.ProtocolSnapshot {
  @Override
  public String getPath() {
    return "s3://bucket/table";
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public List<String> getPartitionColumnNames() {
    return List.of();
  }

  @Override
  public long getTimestamp(Engine engine) {
    return version;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.empty();
  }

  @Override
  public Map<String, String> getTableProperties() {
    return properties;
  }

  @Override
  public SnapshotStatistics getStatistics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    throw new UnsupportedOperationException();
  }

  @Override
  public UpdateTableTransactionBuilder buildUpdateTableTransaction(
      String engineInfo, Operation operation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Snapshot publish(Engine engine) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChecksum(Engine engine, ChecksumWriteMode checksumWriteMode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeCheckpoint(Engine engine) throws IOException {
    throw new UnsupportedOperationException();
  }
}
