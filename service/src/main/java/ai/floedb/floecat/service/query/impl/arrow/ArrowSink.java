package ai.floedb.floecat.service.query.impl.arrow;

import ai.floedb.floecat.system.rpc.ScanSystemTableChunk;
import io.smallrye.mutiny.subscription.MultiEmitter;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Sink that knows how to emit an {@link ArrowScanPlan} over a gRPC stream.
 *
 * <p>Implementations are responsible for serializing the schema/batches, draining the batches
 * iterator, and closing the allocator/plan when streaming completes or errors.
 */
public interface ArrowSink {
  void sink(
      MultiEmitter<? super ScanSystemTableChunk> emitter,
      ArrowScanPlan plan,
      BufferAllocator allocator,
      Function<Throwable, Throwable> errorMapper);
}
