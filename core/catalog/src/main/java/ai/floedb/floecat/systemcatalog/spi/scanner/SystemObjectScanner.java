package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Scanner for system objects (information_schema, floecat.system.*, plugin-provided).
 *
 * <p>Performance constraints: - Must be lazy - Must avoid allocations - Must avoid boxing in the
 * hot path - Must match SchemaColumn[] exactly on every scan
 */
public interface SystemObjectScanner {

  /** Returns the fixed schema (Arrow compatible). */
  List<SchemaColumn> schema();

  /** Lazily streams rows as lightweight wrappers around Object[]. */
  default Stream<SystemObjectRow> scan(SystemObjectScanContext ctx) {
    throw new UnsupportedOperationException("Row scan not implemented");
  }

  /** Arrow-native scan. */
  default Stream<ColumnarBatch> scanArrow(
      SystemObjectScanContext ctx,
      Expr predicate,
      List<String> requiredColumns,
      BufferAllocator allocator) {
    throw new UnsupportedOperationException("Arrow scan not implemented");
  }

  /** Declares supported execution formats. */
  default EnumSet<ScanOutputFormat> supportedFormats() {
    return EnumSet.of(ScanOutputFormat.ROWS);
  }
}
