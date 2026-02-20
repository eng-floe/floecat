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

package ai.floedb.floecat.systemcatalog.columnar;

import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.arrow.SimpleColumnarBatch;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Vectorized filter processor for {@link ColumnarBatch} inputs.
 *
 * <p>The caller hands ownership of {@code batch} into {@link #filter}. The filter closes the input
 * unless it is returned directly (all rows pass). Any returned batch owns its {@link
 * VectorSchemaRoot} and must be closed by the caller; inputs returned empty have a new root that
 * also must be closed.
 */
public final class ArrowFilterOperator {

  private ArrowFilterOperator() {}

  public static ColumnarBatch filter(ColumnarBatch batch, Expr expr, BufferAllocator allocator) {
    if (expr == null) {
      return batch;
    }

    VectorSchemaRoot root = batch.root();
    try (BitVector mask = ArrowExprEvaluator.evaluate(root, expr, allocator)) {
      int rowCount = root.getRowCount();
      int[] selectedRows = new int[rowCount];
      int selectedCount = 0;
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (mask.get(rowIndex) != 0) {
          selectedRows[selectedCount++] = rowIndex;
        }
      }

      if (selectedCount == rowCount) {
        return batch;
      }

      if (selectedCount == 0) {
        batch.close();
        VectorSchemaRoot emptyRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
        emptyRoot.allocateNew();
        emptyRoot.setRowCount(0);
        for (FieldVector vector : emptyRoot.getFieldVectors()) {
          vector.setValueCount(0);
        }
        return new SimpleColumnarBatch(emptyRoot);
      }

      VectorSchemaRoot filteredRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
      boolean copied = false;
      try {
        copySelection(root, filteredRoot, selectedRows, selectedCount);
        copied = true;
        batch.close();
        return new SimpleColumnarBatch(filteredRoot);
      } finally {
        if (!copied) {
          filteredRoot.close();
        }
      }
    }
  }

  private static void copySelection(
      VectorSchemaRoot src, VectorSchemaRoot dst, int[] selectedRows, int selectedCount) {
    List<FieldVector> srcVectors = src.getFieldVectors();
    List<FieldVector> dstVectors = dst.getFieldVectors();
    for (FieldVector vector : dstVectors) {
      vector.allocateNew();
    }

    for (int columnIndex = 0; columnIndex < srcVectors.size(); columnIndex++) {
      FieldVector source = srcVectors.get(columnIndex);
      FieldVector target = dstVectors.get(columnIndex);
      for (int i = 0; i < selectedCount; i++) {
        target.copyFromSafe(selectedRows[i], i, source);
      }
      target.setValueCount(selectedCount);
    }
    dst.setRowCount(selectedCount);
  }

  private static final class ArrowExprEvaluator {

    private static final IntBinaryOperator AND_OPERATION =
        (left, right) -> (left != 0 && right != 0) ? 1 : 0;
    private static final IntBinaryOperator OR_OPERATION =
        (left, right) -> (left != 0 || right != 0) ? 1 : 0;

    private final VectorSchemaRoot root;
    private final BufferAllocator allocator;
    private final Map<String, FieldVector> vectorsByName;
    private final int rowCount;

    private ArrowExprEvaluator(VectorSchemaRoot root, BufferAllocator allocator) {
      this.root = root;
      this.allocator = allocator;
      this.rowCount = root.getRowCount();
      Map<String, FieldVector> columns = new HashMap<>();
      for (FieldVector vector : root.getFieldVectors()) {
        columns.put(vector.getField().getName().toLowerCase(Locale.ROOT), vector);
      }
      this.vectorsByName = columns;
    }

    private static BitVector evaluate(VectorSchemaRoot root, Expr expr, BufferAllocator allocator) {
      ArrowExprEvaluator evaluator = new ArrowExprEvaluator(root, allocator);
      return evaluator.evaluateExpr(expr);
    }

    private BitVector evaluateExpr(Expr expr) {
      return switch (expr) {
        case Expr.And and -> combine(and.left(), and.right(), AND_OPERATION);
        case Expr.Or or -> combine(or.left(), or.right(), OR_OPERATION);
        case Expr.Not not -> invert(evaluateExpr(not.expression()));
        case Expr.Eq eq -> equalityMask(eq);
        case Expr.Gt gt -> greaterThanMask(gt);
        case Expr.IsNull isNull -> isNullMask(isNull.expression());
        default -> throw new UnsupportedOperationException("Expr not supported: " + expr);
      };
    }

    private BitVector equalityMask(Expr.Eq expr) {
      ColumnLiteral pair = columnLiteralPair(expr.left(), expr.right());
      if (pair == null || pair.literal() == null) {
        return falseMask();
      }
      FieldVector vector = pair.column();
      String literal = pair.literal();
      if (vector instanceof IntVector intVector) {
        Integer value = parseInteger(literal);
        if (value == null) {
          return falseMask();
        }
        return compareInt(intVector, value);
      }
      if (vector instanceof BigIntVector bigIntVector) {
        Long value = parseLong(literal);
        if (value == null) {
          return falseMask();
        }
        return compareLong(bigIntVector, value);
      }
      if (vector instanceof Float4Vector float4Vector) {
        Float value = parseFloat(literal);
        if (value == null) {
          return falseMask();
        }
        return compareFloat(float4Vector, value);
      }
      if (vector instanceof Float8Vector float8Vector) {
        Double value = parseDouble(literal);
        if (value == null) {
          return falseMask();
        }
        return compareDouble(float8Vector, value);
      }
      if (vector instanceof VarCharVector varCharVector) {
        byte[] literalBytes = literal.getBytes(StandardCharsets.UTF_8);
        return compareVarChar(varCharVector, literalBytes);
      }
      if (vector instanceof BitVector bitVector) {
        Boolean value = parseBoolean(literal);
        if (value == null) {
          return falseMask();
        }
        return compareBit(bitVector, value);
      }
      return falseMask();
    }

    private BitVector greaterThanMask(Expr.Gt expr) {
      ColumnLiteral pair = columnLiteralPair(expr.left(), expr.right());
      if (pair == null || pair.literal() == null) {
        return falseMask();
      }
      FieldVector vector = pair.column();
      String literal = pair.literal();
      if (vector instanceof IntVector intVector) {
        Integer value = parseInteger(literal);
        if (value == null) {
          return falseMask();
        }
        return greaterThanInt(intVector, value);
      }
      if (vector instanceof BigIntVector bigIntVector) {
        Long value = parseLong(literal);
        if (value == null) {
          return falseMask();
        }
        return greaterThanLong(bigIntVector, value);
      }
      if (vector instanceof Float4Vector float4Vector) {
        Float value = parseFloat(literal);
        if (value == null) {
          return falseMask();
        }
        return greaterThanFloat(float4Vector, value);
      }
      if (vector instanceof Float8Vector float8Vector) {
        Double value = parseDouble(literal);
        if (value == null) {
          return falseMask();
        }
        return greaterThanDouble(float8Vector, value);
      }
      return falseMask();
    }

    private BitVector isNullMask(Expr expr) {
      FieldVector vector = columnVector(expr);
      if (vector == null) {
        return falseMask();
      }
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        mask.setSafe(rowIndex, vector.isNull(rowIndex) ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector combine(Expr leftExpr, Expr rightExpr, IntBinaryOperator operation) {
      BitVector leftMask = evaluateExpr(leftExpr);
      BitVector rightMask = evaluateExpr(rightExpr);
      BitVector result = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        result.setSafe(
            rowIndex, operation.applyAsInt(leftMask.get(rowIndex), rightMask.get(rowIndex)));
      }
      result.setValueCount(rowCount);
      leftMask.close();
      rightMask.close();
      return result;
    }

    private BitVector invert(BitVector mask) {
      BitVector result = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        result.setSafe(rowIndex, mask.get(rowIndex) == 0 ? 1 : 0);
      }
      result.setValueCount(rowCount);
      mask.close();
      return result;
    }

    private BitVector compareInt(IntVector vector, int literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, vector.get(rowIndex) == literal ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector compareLong(BigIntVector vector, long literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, vector.get(rowIndex) == literal ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    /**
     * Floating-point equality follows Java/IEEE semantics. NaN never matches itself and infinities
     * match only their like-sign counterparts; invalid literals produce {@code null} and therefore
     * no matches.
     */
    private BitVector compareFloat(Float4Vector vector, float literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, vector.get(rowIndex) == literal ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector compareDouble(Float8Vector vector, double literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, vector.get(rowIndex) == literal ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector compareVarChar(VarCharVector vector, byte[] literal) {
      BitVector mask = newMask();
      // Avoid VarCharVector.get(i) because it allocates a new byte[] for each row.
      // Instead, compare the underlying data buffer slice [start, end) against the literal bytes.
      ArrowBuf offsets = vector.getOffsetBuffer();
      ArrowBuf data = vector.getDataBuffer();

      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }

        int start = offsets.getInt((long) rowIndex * 4);
        int end = offsets.getInt((long) (rowIndex + 1) * 4);
        mask.setSafe(rowIndex, equalsSlice(data, start, end, literal) ? 1 : 0);
      }

      mask.setValueCount(rowCount);
      return mask;
    }

    private static boolean equalsSlice(ArrowBuf data, int start, int end, byte[] literal) {
      int len = end - start;
      if (len != literal.length) {
        return false;
      }
      // Compare byte-by-byte without allocating.
      long pos = start;
      for (int i = 0; i < len; i++) {
        if (data.getByte(pos + i) != literal[i]) {
          return false;
        }
      }
      return true;
    }

    private BitVector compareBit(BitVector vector, boolean literal) {
      BitVector mask = newMask();
      int literalBit = literal ? 1 : 0;
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, vector.get(rowIndex) == literalBit ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector greaterThanInt(IntVector vector, int literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex) || vector.get(rowIndex) <= literal) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, 1);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector greaterThanLong(BigIntVector vector, long literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex) || vector.get(rowIndex) <= literal) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, 1);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector greaterThanFloat(Float4Vector vector, float literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex) || vector.get(rowIndex) <= literal) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, 1);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private BitVector greaterThanDouble(Float8Vector vector, double literal) {
      BitVector mask = newMask();
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex) || vector.get(rowIndex) <= literal) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        mask.setSafe(rowIndex, 1);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private FieldVector columnVector(Expr expr) {
      if (!(expr instanceof Expr.ColumnRef columnRef)) {
        return null;
      }
      String name = columnRef.name();
      if (name == null) {
        return null;
      }
      String normalized = name.trim().toLowerCase(Locale.ROOT);
      return vectorsByName.get(normalized);
    }

    private ColumnLiteral columnLiteralPair(Expr left, Expr right) {
      if (left instanceof Expr.ColumnRef column && right instanceof Expr.Literal literal) {
        FieldVector vector = columnVector(column);
        return vector == null ? null : new ColumnLiteral(vector, literal.value());
      }
      if (right instanceof Expr.ColumnRef column && left instanceof Expr.Literal literal) {
        FieldVector vector = columnVector(column);
        return vector == null ? null : new ColumnLiteral(vector, literal.value());
      }
      return null;
    }

    private BitVector newMask() {
      BitVector mask = new BitVector("selection", allocator);
      mask.allocateNew(rowCount);
      return mask;
    }

    private BitVector falseMask() {
      BitVector mask = newMask();
      for (int i = 0; i < rowCount; i++) {
        mask.setSafe(i, 0);
      }
      mask.setValueCount(rowCount);
      return mask;
    }

    private static Integer parseInteger(String value) {
      if (value == null) {
        return null;
      }
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static Long parseLong(String value) {
      if (value == null) {
        return null;
      }
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static Float parseFloat(String value) {
      if (value == null) {
        return null;
      }
      try {
        return Float.parseFloat(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    /**
     * Parses numeric literals exactly; invalid values (including unsupported representations)
     * return {@code null} so the corresponding predicate becomes an empty mask.
     */
    private static Double parseDouble(String value) {
      if (value == null) {
        return null;
      }
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static Boolean parseBoolean(String value) {
      if (value == null) {
        return null;
      }
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      return switch (normalized) {
        case "true" -> Boolean.TRUE;
        case "t" -> Boolean.TRUE;
        case "false" -> Boolean.FALSE;
        case "f" -> Boolean.FALSE;
        default -> null;
      };
    }

    private record ColumnLiteral(FieldVector column, String literal) {}
  }
}
