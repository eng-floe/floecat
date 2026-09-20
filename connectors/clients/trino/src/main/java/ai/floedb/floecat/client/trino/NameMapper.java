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

package ai.floedb.floecat.client.trino;

import ai.floedb.floecat.common.rpc.NameRef;
import io.trino.spi.connector.SchemaTableName;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

final class NameMapper {
  private NameMapper() {}

  static String schemaFrom(NameRef ref) {
    List<String> parts =
        new ArrayList<>(ref.getPathList().stream().filter(p -> !p.isBlank()).toList());
    return parts.stream().collect(Collectors.joining("."));
  }

  static NameRef prefix(String catalog, String schema) {
    NameRef.Builder b = NameRef.newBuilder().setCatalog(catalog);
    if (schema != null && !schema.isBlank()) {
      b.addAllPath(List.of(schema.split("\\.")));
    }
    return b.build();
  }

  static NameRef nameRef(String catalog, String schema, String table) {
    NameRef.Builder b = prefix(catalog, schema).toBuilder();
    b.setName(table);
    return b.build();
  }

}
