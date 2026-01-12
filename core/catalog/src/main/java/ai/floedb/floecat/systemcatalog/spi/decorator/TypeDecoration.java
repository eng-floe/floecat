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

package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.query.rpc.SqlType;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.Objects;

/** Mutable holder describing a type (SqlType) during bundle decoration. */
public final class TypeDecoration extends AbstractDecoration {

  private final SqlType.Builder builder;

  public TypeDecoration(SqlType.Builder builder, MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.builder = Objects.requireNonNull(builder, "builder");
  }

  public SqlType.Builder builder() {
    return builder;
  }
}
