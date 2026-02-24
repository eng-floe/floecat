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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Utility that builds an {@link ArrowScanPlan} from an in-memory list of Java records. */
public final class RecordArrowScanPlan {

  private RecordArrowScanPlan() {}

  public static <T extends Record> ArrowScanPlan of(
      ArrowRecordWriter<T> writer, List<T> rows, BufferAllocator allocator) {
    Objects.requireNonNull(writer, "writer");
    Objects.requireNonNull(rows, "rows");
    Objects.requireNonNull(allocator, "allocator");
    List<T> copy = List.copyOf(rows);
    ColumnarBatch batch = new RecordColumnarBatch<>(writer, copy, allocator);
    return ArrowScanPlan.of(writer.schema(), Stream.of(batch));
  }

  public static <T extends Record> ArrowScanPlan paged(
      ArrowRecordWriter<T> writer,
      Iterator<? extends List<T>> pages,
      BufferAllocator allocator,
      BooleanSupplier cancelled) {
    Objects.requireNonNull(writer, "writer");
    Objects.requireNonNull(pages, "pages");
    Objects.requireNonNull(allocator, "allocator");
    Iterator<ColumnarBatch> batchIterator =
        new PagedColumnarBatchIterator<>(writer, pages, allocator, cancelled);
    Stream<ColumnarBatch> stream =
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(batchIterator, Spliterator.ORDERED), false);
    return ArrowScanPlan.of(writer.schema(), stream);
  }

  private static final class RecordColumnarBatch<T extends Record> implements ColumnarBatch {

    private final ArrowRecordWriter<T> writer;
    private final VectorSchemaRoot root;
    private boolean closed;

    RecordColumnarBatch(ArrowRecordWriter<T> writer, List<T> rows, BufferAllocator allocator) {
      this.writer = writer;
      this.root = VectorSchemaRoot.create(writer.schema(), allocator);
      this.root.allocateNew();
      writer.write(root, rows);
    }

    @Override
    public VectorSchemaRoot root() {
      return root;
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        root.close();
      }
    }
  }

  private static final class PagedColumnarBatchIterator<T extends Record>
      implements Iterator<ColumnarBatch> {

    private final ArrowRecordWriter<T> writer;
    private final Iterator<? extends List<T>> pages;
    private final BufferAllocator allocator;
    private final BooleanSupplier cancelled;

    PagedColumnarBatchIterator(
        ArrowRecordWriter<T> writer, Iterator<? extends List<T>> pages, BufferAllocator allocator) {
      this.writer = writer;
      this.pages = pages;
      this.allocator = allocator;
      this.cancelled = () -> false;
    }

    PagedColumnarBatchIterator(
        ArrowRecordWriter<T> writer,
        Iterator<? extends List<T>> pages,
        BufferAllocator allocator,
        BooleanSupplier cancelled) {
      this.writer = writer;
      this.pages = pages;
      this.allocator = allocator;
      this.cancelled = cancelled;
    }

    @Override
    public boolean hasNext() {
      if (cancelled.getAsBoolean()) {
        return false;
      }
      return pages.hasNext();
    }

    @Override
    public ColumnarBatch next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException();
      }
      List<T> rows = pages.next();
      return new RecordColumnarBatch<>(writer, rows, allocator);
    }
  }
}
