package ai.floedb.metacat.reconciler.util;

import java.util.*;

public final class NameParts {
  private NameParts() {}

  public static class Ns {
    public final List<String> parents;
    public final String leaf;

    public Ns(List<String> parents, String leaf) {
      this.parents = parents;
      this.leaf = leaf;
    }
  }

  public static Ns split(String namespaceFq) {
    if (namespaceFq == null || namespaceFq.isBlank()) {
      return new Ns(List.of(), "");
    }
    var parts = List.of(namespaceFq.split("\\."));
    if (parts.size() == 1) {
      return new Ns(List.of(), parts.get(0));
    }
    return new Ns(parts.subList(0, parts.size()-1), parts.get(parts.size()-1));
  }
}
