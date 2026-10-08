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

package ai.floedb.floecat.scanner.spi;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.scanner.expr.Expr;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemScanRequestTest {

  @Test
  void needs_columnsThePredicateReads() {
    Expr predicate =
        new Expr.Not(
            new Expr.And(
                new Expr.Eq(new Expr.ColumnRef("Kind"), new Expr.Literal("table")),
                new Expr.IsNull(new Expr.ColumnRef("owner"))));
    SystemScanRequest request = SystemScanRequest.of(predicate, List.of("name"));

    assertThat(request.needs("kind")).isTrue();
    assertThat(request.needs("owner")).isTrue();
    assertThat(request.needs("name")).isTrue();
    assertThat(request.needs("id")).isFalse();
  }
}
