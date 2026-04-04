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

package ai.floedb.floecat.reconciler.spi;

import ai.floedb.floecat.common.rpc.NameRef;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public final class NameRefNormalizer {
  private NameRefNormalizer() {}

  public static NameRef normalize(NameRef ref) {
    if (ref == null) {
      return null;
    }
    NameRef.Builder builder = NameRef.newBuilder();
    if (ref.hasResourceId()) {
      builder.setResourceId(ref.getResourceId());
    }
    String catalog = normalizeDisplayName(ref.getCatalog());
    if (!catalog.isBlank()) {
      builder.setCatalog(catalog);
    }
    builder.addAllPath(normalizePath(ref.getPathList()));
    String name = normalizeDisplayName(ref.getName());
    if (!name.isBlank()) {
      builder.setName(name);
    }
    return builder.build();
  }

  public static String normalizeDisplayName(String input) {
    if (input == null) {
      return "";
    }
    String normalized = Normalizer.normalize(input.strip(), Normalizer.Form.NFKC);
    return normalized.replaceAll("\\s+", " ");
  }

  public static List<String> normalizeNamespacePath(String namespaceFq) {
    if (namespaceFq == null || namespaceFq.isBlank()) {
      return List.of();
    }
    return normalizePath(List.of(namespaceFq.split("\\.")));
  }

  private static List<String> normalizePath(List<String> segments) {
    if (segments == null || segments.isEmpty()) {
      return List.of();
    }
    List<String> normalized = new ArrayList<>(segments.size());
    for (String segment : segments) {
      if (segment == null) {
        continue;
      }
      String value = normalizeDisplayName(segment);
      if (!value.isBlank()) {
        normalized.add(value);
      }
    }
    return normalized;
  }
}
