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
