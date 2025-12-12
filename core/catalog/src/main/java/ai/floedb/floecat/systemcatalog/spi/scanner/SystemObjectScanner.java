package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;
import java.util.stream.Stream;

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
  Stream<SystemObjectRow> scan(SystemObjectScanContext ctx);
}
