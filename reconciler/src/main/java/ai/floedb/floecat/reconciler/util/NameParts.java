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

package ai.floedb.floecat.reconciler.util;

import java.util.List;

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
    return new Ns(parts.subList(0, parts.size() - 1), parts.get(parts.size() - 1));
  }
}
