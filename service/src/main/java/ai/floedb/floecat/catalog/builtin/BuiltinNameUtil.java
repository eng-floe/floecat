package ai.floedb.floecat.catalog.builtin;

import ai.floedb.floecat.common.rpc.NameRef;

public final class BuiltinNameUtil {
  private BuiltinNameUtil() {}

  public static String canonical(NameRef ref) {
    if (ref == null) return "";
    String path = String.join(".", ref.getPathList());
    return path.isEmpty() ? ref.getName() : path + "." + ref.getName();
  }
}
