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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlightArrowBatchSinkTest {
  private static final Schema SCHEMA =
      new Schema(
          List.of(new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

  private BufferAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void doesNotRecordBatchAndRowsWhenPutNextThrows() {
    FlightArrowBatchSink sink = new FlightArrowBatchSink(new ThrowingPutNextListener(), allocator);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      IntVector vector = (IntVector) root.getVector("value");
      vector.allocateNew(2);
      vector.setSafe(0, 10);
      vector.setSafe(1, 20);
      root.setRowCount(2);

      assertThrows(IllegalStateException.class, () -> sink.onBatch(root));
      assertEquals(0, sink.batches());
      assertEquals(0L, sink.rows());
    } finally {
      sink.close();
    }
  }

  private static final class ThrowingPutNextListener
      implements FlightProducer.ServerStreamListener {
    @Override
    public void start(VectorSchemaRoot root) {}

    @Override
    public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {}

    @Override
    public void putNext() {
      throw new IllegalStateException("putNext failed");
    }

    @Override
    public void putNext(ArrowBuf metadata) {
      throw new IllegalStateException("putNext failed");
    }

    @Override
    public void putMetadata(ArrowBuf metadata) {}

    @Override
    public void error(Throwable t) {}

    @Override
    public void completed() {}

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {}

    @Override
    public void setOnCancelHandler(Runnable handler) {}

    @Override
    public void setUseZeroCopy(boolean enabled) {}

    @Override
    public boolean isCancelled() {
      return false;
    }
  }
}
