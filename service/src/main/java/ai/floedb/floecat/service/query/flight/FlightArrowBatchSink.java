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

package ai.floedb.floecat.service.query.flight;

import ai.floedb.floecat.service.query.impl.arrow.ArrowBatchSink;
import java.util.Objects;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * {@link ArrowBatchSink} implementation that writes Arrow batches natively to a Flight stream via
 * {@link ServerStreamListener}.
 *
 * <p>This sink reuses a single {@link VectorSchemaRoot} for the entire stream, copying incoming
 * batches into it before calling {@code putNext()}. Keeping the same root is required by the Arrow
 * Flight contract and avoids sending stale/closed buffers when the underlying scanner emits a new
 * {@link VectorSchemaRoot} for each batch.
 */
final class FlightArrowBatchSink implements ArrowBatchSink {

  private final ServerStreamListener listener;
  private final BufferAllocator allocator;
  private Schema schema;
  private VectorSchemaRoot schemaRoot;
  private VectorLoader loader;
  private boolean started;

  FlightArrowBatchSink(ServerStreamListener listener, BufferAllocator allocator) {
    this.listener = Objects.requireNonNull(listener, "listener");
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  @Override
  public void onSchema(Schema schema) {
    this.schema = schema;
    ensureSchemaRoot(null);
  }

  private void ensureSchemaRoot(VectorSchemaRoot fallbackRoot) {
    if (schemaRoot != null) {
      return;
    }
    Schema effectiveSchema = schema;
    if (effectiveSchema == null && fallbackRoot != null) {
      effectiveSchema = fallbackRoot.getSchema();
    }
    if (effectiveSchema == null) {
      throw new IllegalStateException("Flight schema root cannot be created");
    }
    schemaRoot = VectorSchemaRoot.create(effectiveSchema, allocator);
    loader = new VectorLoader(schemaRoot);
  }

  @Override
  public void onBatch(VectorSchemaRoot root) {
    ensureSchemaRoot(root);
    schemaRoot.clear();
    schemaRoot.allocateNew();
    try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
      loader.load(recordBatch);
      schemaRoot.setRowCount(recordBatch.getLength());
    }
    ensureStarted();
    listener.putNext();
  }

  private void ensureStarted() {
    if (!started) {
      listener.start(schemaRoot);
      started = true;
    }
  }

  @Override
  public void onComplete() {
    if (!started) {
      ensureSchemaRoot(null);
      schemaRoot.clear();
      schemaRoot.allocateNew();
      schemaRoot.setRowCount(0);
      ensureStarted();
    }
    listener.completed();
  }

  void close() {
    if (schemaRoot != null) {
      try {
        schemaRoot.close();
      } finally {
        schemaRoot = null;
      }
    }
  }
}
