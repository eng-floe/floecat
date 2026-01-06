package ai.floedb.floecat.service.query.impl.arrow;

import ai.floedb.floecat.system.rpc.ScanSystemTableChunk;
import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import com.google.protobuf.ByteString;
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

/**
 * Writes the ArrowScanPlan to the gRPC stream by serializing the schema once, then each record
 * batch.
 *
 * <p>The sink also registers cleanup handlers to close the plan and allocator when the stream
 * terminates or is cancelled.
 */
public final class GrpcArrowSink implements ArrowSink {
  @Override
  public void sink(
      MultiEmitter<? super ScanSystemTableChunk> emitter,
      ArrowScanPlan plan,
      BufferAllocator allocator,
      Function<Throwable, Throwable> errorMapper) {
    AtomicBoolean closed = new AtomicBoolean(false);
    Runnable cleanup =
        () -> {
          if (closed.compareAndSet(false, true)) {
            try {
              plan.close();
            } catch (Exception e) {
              // ignore
            }
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
      emitter.emit(
          ScanSystemTableChunk.newBuilder()
              .setArrowSchemaIpc(serializeSchema(plan.schema()))
              .build());
      Iterator<ColumnarBatch> iterator = plan.iterator();
      while (!emitter.isCancelled() && iterator.hasNext()) {
        ColumnarBatch batch = iterator.next();
        try {
          emitter.emit(
              ScanSystemTableChunk.newBuilder().setArrowBatchIpc(serializeBatch(batch)).build());
        } finally {
          batch.close();
        }
      }
      if (!emitter.isCancelled()) {
        emitter.complete();
      }
    } catch (Throwable t) {
      emitter.fail(errorMapper.apply(t));
    }
  }

  /** Serializes a schema into an Arrow IPC message. */
  private static ByteString serializeSchema(org.apache.arrow.vector.types.pojo.Schema schema) {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
      return ByteString.copyFrom(out.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize Arrow schema", e);
    }
  }

  /** Serializes a record batch into an Arrow IPC message. */
  private static ByteString serializeBatch(ColumnarBatch batch) {
    VectorSchemaRoot root = batch.root();
    try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), recordBatch);
      return ByteString.copyFrom(out.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize Arrow record batch", e);
    }
  }
}
