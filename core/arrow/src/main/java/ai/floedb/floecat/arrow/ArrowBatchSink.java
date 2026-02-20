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

package ai.floedb.floecat.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Protocol-neutral sink for Arrow batch streaming.
 *
 * <p>Implementations write the schema and batches to a specific wire protocol (gRPC chunks or Arrow
 * Flight). Using a shared sink interface keeps the serialisation loop in one place ({@link
 * ArrowBatchSerializer}), so any future optimisation (compression, batch size tuning, statistics
 * headers) applies to all transports automatically.
 *
 * <p>{@code onSchema} is always called with the <em>projected</em> schema (matching what {@code
 * onBatch} will send). It is called exactly once before any {@code onBatch} call.
 */
public interface ArrowBatchSink {

  /** Called once with the projected schema before any batches are emitted. */
  void onSchema(Schema schema);

  /**
   * Called for each record batch. The root is owned by the caller; implementations must not close
   * it and must not retain a reference after this method returns.
   */
  void onBatch(VectorSchemaRoot root);

  /** Called when the stream is complete with no errors. */
  void onComplete();
}
