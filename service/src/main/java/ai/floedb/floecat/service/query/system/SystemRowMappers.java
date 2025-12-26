package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.system.rpc.SystemTableRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;

public final class SystemRowMappers {

  public static SystemTableRow toProto(SystemObjectRow row) {
    var b = SystemTableRow.newBuilder();
    for (Object v : row.values()) {
      b.addValues(v == null ? "" : v.toString());
    }
    return b.build();
  }

  private SystemRowMappers() {}
}
