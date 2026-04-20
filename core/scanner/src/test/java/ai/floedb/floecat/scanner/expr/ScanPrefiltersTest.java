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

package ai.floedb.floecat.scanner.expr;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

class ScanPrefiltersTest {

  @Test
  void extractsConjunctionOfEqConstraints() {
    Expr predicate =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("table_name"), new Expr.Literal("orders")),
            new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("42")));

    ScanPrefilter prefilter =
        ScanPrefilters.fromPredicate(predicate, Set.of("table_name", "snapshot_id"));

    assertThat(prefilter.impossible()).isFalse();
    assertThat(prefilter.valuesFor("table_name")).containsExactly("orders");
    assertThat(prefilter.longFor("snapshot_id")).hasValue(42L);
  }

  @Test
  void contradictoryNumericEqualsAreImpossible() {
    Expr predicate =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("1")),
            new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("2")));

    ScanPrefilter prefilter = ScanPrefilters.fromPredicate(predicate, Set.of("snapshot_id"));

    assertThat(prefilter.impossible()).isTrue();
  }

  @Test
  void unsupportedColumnsAreIgnored() {
    Expr predicate =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("table_name"), new Expr.Literal("orders")),
            new Expr.Eq(new Expr.ColumnRef("other"), new Expr.Literal("value")));

    ScanPrefilter prefilter = ScanPrefilters.fromPredicate(predicate, Set.of("table_name"));

    assertThat(prefilter.impossible()).isFalse();
    assertThat(prefilter.valuesFor("table_name")).containsExactly("orders");
    assertThat(prefilter.valuesFor("other")).isEmpty();
  }
}
