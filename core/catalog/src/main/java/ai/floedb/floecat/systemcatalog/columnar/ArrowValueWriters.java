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

package ai.floedb.floecat.systemcatalog.columnar;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.arrow.vector.VarCharVector;

/** Lightweight helpers for writing values into Arrow vectors. */
public final class ArrowValueWriters {

  private ArrowValueWriters() {}

  public static void writeVarChar(VarCharVector vector, int index, String value) {
    Objects.requireNonNull(vector, "vector");
    if (value == null) {
      vector.setNull(index);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    vector.setSafe(index, bytes);
  }
}
