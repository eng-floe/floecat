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

package ai.floedb.floecat.flight;

import ai.floedb.floecat.arrow.ArrowBatchSink;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.arrow.flight.CallStatus;
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
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  FlightArrowBatchSink(ServerStreamListener listener, BufferAllocator allocator) {
    this.listener = Objects.requireNonNull(listener, "listener");
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    this.listener.setOnCancelHandler(() -> cancelled.set(true));
  }

  @Override
  public void onSchema(Schema schema) {
    System.err.println("onSchema: schema=" + schema.getFields().size() + " fields");
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
    System.err.println(
        "onBatch: root="
            + System.identityHashCode(root)
            + ", cancelled="
            + cancelled.get()
            + ", listener.isCancelled="
            + listener.isCancelled());
    ensureSchemaRoot(root);
    schemaRoot.clear();
    System.err.println("onBatch: allocateNew, schemaRoot=" + System.identityHashCode(schemaRoot));
    schemaRoot.allocateNew();
    try {
      try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
        loader.load(recordBatch);
        schemaRoot.setRowCount(recordBatch.getLength());
      }
      ensureStarted();
      waitForReady();
      listener.putNext();
    } catch (Throwable t) {
      System.err.println(
          "onBatch: error caught (cancelled="
              + cancelled.get()
              + ", listener.isCancelled="
              + listener.isCancelled()
              + "), closing schemaRoot");
      // On error, fully close and release the schemaRoot to free all buffers
      if (schemaRoot != null) {
        try {
          System.err.println(
              "onBatch: clearing vectors before close, schemaRoot="
                  + System.identityHashCode(schemaRoot));
          schemaRoot.clear();
        } catch (Exception ignored) {
          System.err.println("onBatch: error clearing schemaRoot: " + ignored.getMessage());
        }
        try {
          System.err.println("onBatch: closing schemaRoot=" + System.identityHashCode(schemaRoot));
          schemaRoot.close();
        } catch (Exception ignored) {
          System.err.println("onBatch: error closing schemaRoot: " + ignored.getMessage());
        } finally {
          schemaRoot = null;
          loader = null;
        }
      }
      throw t;
    }
  }

  private void waitForReady() {
    long backoffNanos = 100_000;
    while (!listener.isReady()) {
      if (cancelled.get() || listener.isCancelled()) {
        throw CallStatus.CANCELLED.withDescription("Flight stream cancelled").toRuntimeException();
      }
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw CallStatus.CANCELLED
            .withDescription("Flight stream interrupted")
            .withCause(new InterruptedException())
            .toRuntimeException();
      }
      LockSupport.parkNanos(backoffNanos);
      backoffNanos = Math.min(backoffNanos * 2, 5_000_000);
    }
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
  }

  void close() {
    System.err.println(
        "FlightArrowBatchSink.close: schemaRoot="
            + (schemaRoot == null ? "null" : System.identityHashCode(schemaRoot)));
    if (schemaRoot != null) {
      try {
        // Clear all buffers first to explicitly release storage
        schemaRoot.clear();
      } catch (Exception ignored) {
      }
      try {
        System.err.println("FlightArrowBatchSink.close: closing schemaRoot");
        schemaRoot.close();
      } catch (Exception ignored) {
      } finally {
        schemaRoot = null;
      }
    }
  }
}
