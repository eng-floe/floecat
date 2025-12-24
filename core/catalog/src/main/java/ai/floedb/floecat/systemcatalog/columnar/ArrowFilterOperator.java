package ai.floedb.floecat.systemcatalog.columnar;

import ai.floedb.floecat.systemcatalog.expr.Expr;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntBinaryOperator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Vectorized filter processor for {@link ColumnarBatch} inputs. */
public final class ArrowFilterOperator {

  private ArrowFilterOperator() {}

  public static ColumnarBatch filter(ColumnarBatch batch, Expr expr, BufferAllocator allocator) {
    if (expr == null) {
      return batch;
    }

    VectorSchemaRoot root = batch.root();
    try (BitVector mask = ArrowExprEvaluator.evaluate(root, expr, allocator)) {
      VectorSchemaRoot filteredRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
      boolean copied = false;
      try {
        copySelection(root, filteredRoot, mask);
        copied = true;
        return new SimpleColumnarBatch(filteredRoot);
      } finally {
        if (!copied) {
          filteredRoot.close();
        }
      }
    } finally {
      batch.close();
    }
  }

  private static void copySelection(
      VectorSchemaRoot src, VectorSchemaRoot dst, BitVector selectionMask) {
    List<FieldVector> srcVectors = src.getFieldVectors();
    List<FieldVector> dstVectors = dst.getFieldVectors();
    for (FieldVector vector : dstVectors) {
      vector.allocateNew();
    }

    int outputRow = 0;
    int rowCount = src.getRowCount();
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      if (selectionMask.get(rowIndex) == 0) {
        continue;
      }
      for (int columnIndex = 0; columnIndex < srcVectors.size(); columnIndex++) {
        FieldVector source = srcVectors.get(columnIndex);
        FieldVector target = dstVectors.get(columnIndex);
        target.copyFromSafe(rowIndex, outputRow, source);
      }
      outputRow++;
    }

    for (FieldVector vector : dstVectors) {
      vector.setValueCount(outputRow);
    }
    dst.setRowCount(outputRow);
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
        Double value = parseDouble(literal);
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
      Double literalValue = parseDouble(pair.literal());
      if (literalValue == null) {
        return falseMask();
      }
      if (vector instanceof IntVector intVector) {
        return greaterThanInt(intVector, literalValue);
      }
      if (vector instanceof BigIntVector bigIntVector) {
        return greaterThanLong(bigIntVector, literalValue);
      }
      if (vector instanceof Float4Vector float4Vector) {
        return greaterThanFloat(float4Vector, literalValue);
      }
      if (vector instanceof Float8Vector float8Vector) {
        return greaterThanDouble(float8Vector, literalValue);
      }
      return falseMask();
    }

    private BitVector isNullMask(Expr expr) {
      FieldVector vector = columnVector(expr);
      BitVector mask = newMask();
      if (vector == null) {
        mask.setValueCount(rowCount);
        return mask;
      }
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

    private BitVector compareFloat(Float4Vector vector, double literal) {
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
      for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        if (vector.isNull(rowIndex)) {
          mask.setSafe(rowIndex, 0);
          continue;
        }
        byte[] value = vector.get(rowIndex);
        mask.setSafe(rowIndex, Arrays.equals(value, literal) ? 1 : 0);
      }
      mask.setValueCount(rowCount);
      return mask;
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

    private BitVector greaterThanInt(IntVector vector, double literal) {
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

    private BitVector greaterThanLong(BigIntVector vector, double literal) {
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

    private BitVector greaterThanFloat(Float4Vector vector, double literal) {
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
      return vectorsByName.get(name.toLowerCase(Locale.ROOT));
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
      return Boolean.parseBoolean(value);
    }

    private record ColumnLiteral(FieldVector column, String literal) {}
  }
}
