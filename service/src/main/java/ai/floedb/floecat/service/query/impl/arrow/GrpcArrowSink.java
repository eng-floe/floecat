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

package ai.floedb.floecat.service.query.impl.arrow;

import ai.floedb.floecat.arrow.ArrowBatchSink;
import ai.floedb.floecat.system.rpc.ScanSystemTableChunk;
import com.google.protobuf.ByteString;
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Writes the ArrowScanPlan to the gRPC stream by serializing the schema once, then each record
 * batch, via {@link ArrowBatchSerializer}.
 *
 * <p>Implements both {@link ArrowSink} (the gRPC entry point) and {@link ArrowBatchSink} (the
 * protocol-neutral sink API driven by {@link ArrowBatchSerializer}). The IPC helpers {@link
 * #serializeSchema} and {@link #serializeBatch} are package-private so they can be reused by tests.
 */
public final class GrpcArrowSink implements ArrowSink, ArrowBatchSink {

  private MultiEmitter<? super ScanSystemTableChunk> emitter;

  @Override
  public void sink(
      MultiEmitter<? super ScanSystemTableChunk> emitter,
      ArrowScanPlan plan,
      BufferAllocator allocator,
      Function<Throwable, Throwable> errorMapper) {
    this.emitter = emitter;

    AtomicBoolean closed = new AtomicBoolean(false);
    Runnable cleanup =
        () -> {
          if (closed.compareAndSet(false, true)) {
            try {
              allocator.close();
            } catch (Exception e) {
              // ignore
            }
          }
        };
    emitter.onTermination(cleanup);
    emitter.onCancellation(cleanup);

    try {
      ArrowBatchSerializer.serialize(plan, this, emitter::isCancelled, cleanup);
    } catch (Throwable t) {
      emitter.fail(errorMapper.apply(t));
    }
  }

  @Override
  public void onSchema(Schema schema) {
    emitter.emit(
        ScanSystemTableChunk.newBuilder().setArrowSchemaIpc(serializeSchema(schema)).build());
  }

  @Override
  public void onBatch(VectorSchemaRoot root) {
    emitter.emit(ScanSystemTableChunk.newBuilder().setArrowBatchIpc(serializeBatch(root)).build());
  }

  @Override
  public void onComplete() {
    emitter.complete();
  }

  /** Serializes a schema into an Arrow IPC message. Package-private for tests. */
  static ByteString serializeSchema(Schema schema) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
      return ByteString.copyFrom(out.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize Arrow schema", e);
    }
  }

  /**
   * Serializes a VectorSchemaRoot into an Arrow IPC record batch message. Package-private for
   * tests.
   */
  static ByteString serializeBatch(VectorSchemaRoot root) {
    try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), recordBatch);
      return ByteString.copyFrom(out.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize Arrow record batch", e);
    }
  }
}
