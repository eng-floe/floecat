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

package ai.floedb.floecat.client.cli.util;

import ai.floedb.floecat.common.rpc.NameRef;
import java.util.List;

/** NameRef parser helpers used across CLI commands. */
public final class NameRefUtil {
  private NameRefUtil() {}

  public static NameRef nameRefForTable(String fq) {
    if (fq == null) {
      throw new IllegalArgumentException("Fully qualified name is required");
    }
    fq = fq.trim();
    if (fq.isEmpty()) {
      throw new IllegalArgumentException(
          "Fully qualified name must contain a catalog and namespace");
    }
    List<String> segs = FQNameParserUtil.segments(fq);
    if (segs.size() < 3) {
      throw new IllegalArgumentException(
          "Invalid table path: at least a catalog, one namespace, and a table name are required"
              + " (e.g. catalog.namespace.table)");
    }
    String catalog = Quotes.unquote(segs.get(0));
    String object = Quotes.unquote(segs.get(segs.size() - 1));
    List<String> ns = segs.subList(1, segs.size() - 1).stream().map(Quotes::unquote).toList();
    return NameRef.newBuilder().setCatalog(catalog).addAllPath(ns).setName(object).build();
  }
}
