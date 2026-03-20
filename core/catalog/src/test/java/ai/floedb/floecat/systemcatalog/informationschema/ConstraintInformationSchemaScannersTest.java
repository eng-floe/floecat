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

package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConstraintInformationSchemaScannersTest {

  @Test
  void tableConstraints_mapsCurrentModelWithNullableAnsiGaps() {
    SystemObjectScanContext ctx = contextWithConstraints();

    var rows = new TableConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .anySatisfy(
            row ->
                assertThat(row)
                    .containsExactly(
                        "catalog",
                        "sales",
                        "pk_orders",
                        "catalog",
                        "sales",
                        "orders",
                        "PRIMARY KEY",
                        "NO",
                        "NO",
                        "YES"));
    assertThat(rows)
        .anySatisfy(
            row ->
                assertThat(row)
                    .containsExactly(
                        "catalog",
                        "sales",
                        "fk_orders_customer",
                        "catalog",
                        "sales",
                        "orders",
                        "FOREIGN KEY",
                        "NO",
                        "NO",
                        "NO"));
  }

  @Test
  void keyColumnUsage_includesPkAndFkOrdinals() {
    SystemObjectScanContext ctx = contextWithConstraints();

    var rows = new KeyColumnUsageScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .anySatisfy(
            row ->
                assertThat(row)
                    .isEqualTo(
                        Arrays.asList(
                            "catalog",
                            "sales",
                            "pk_orders",
                            "catalog",
                            "sales",
                            "orders",
                            "id",
                            1,
                            null)));
    assertThat(rows)
        .anySatisfy(
            row ->
                assertThat(row)
                    .containsExactly(
                        "catalog",
                        "sales",
                        "fk_orders_customer",
                        "catalog",
                        "sales",
                        "orders",
                        "customer_id",
                        1,
                        1));
  }

  @Test
  void keyColumnUsage_excludesNotNullConstraints() {
    SystemObjectScanContext ctx = contextWithOnlyNotNullConstraint();

    var rows = new KeyColumnUsageScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows).isEmpty();
  }

  @Test
  void referentialConstraints_emitsReferencedConstraintAndRules() {
    SystemObjectScanContext ctx = contextWithConstraints();

    var rows =
        new ReferentialConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .isEqualTo(
            Arrays.asList(
                "catalog",
                "sales",
                "fk_orders_customer",
                "catalog",
                "sales",
                "pk_customers",
                "FULL",
                "CASCADE",
                "RESTRICT"));
  }

  @Test
  void referentialConstraints_defaultsAnsiRulesWhenUnspecified() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var sales = builder.addNamespace("sales");
    var orders =
        builder.addTable(
            sales,
            "orders",
            Map.of("id", 1, "customer_id", 2),
            Map.of("id", "INT", "customer_id", "INT"));

    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customer_default_rules")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(column("customer_id", 2L, 1))
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .addReferencedColumns(column("id", 1L, 1))
            .build();

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(orders.id())) {
              return Optional.of(
                  new ConstraintSetView() {
                    @Override
                    public ResourceId relationId() {
                      return relationId;
                    }

                    @Override
                    public List<ConstraintDefinition> constraints() {
                      return List.of(fk);
                    }
                  });
            }
            return Optional.empty();
          }
        };

    SystemObjectScanContext base = builder.build();
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            base.graph(),
            NameRef.getDefaultInstance(),
            base.queryDefaultCatalogId(),
            EngineContext.empty(),
            StatsProvider.NONE,
            provider);

    var rows =
        new ReferentialConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .containsExactly(
            Arrays.asList(
                "catalog",
                "sales",
                "fk_orders_customer_default_rules",
                null,
                null,
                null,
                "NONE",
                "NO ACTION",
                "NO ACTION"));
  }

  @Test
  void checkConstraints_emitsCheckClause() {
    SystemObjectScanContext ctx = contextWithConstraints();

    var rows = new CheckConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows).containsExactly(List.of("catalog", "sales", "ck_orders_total", "total >= 0"));
  }

  @Test
  void constraintColumnUsage_includesLocalAndReferencedColumns() {
    SystemObjectScanContext ctx = contextWithConstraints();

    var rows =
        new ConstraintColumnUsageScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .contains(
            List.of("catalog", "sales", "pk_orders", "catalog", "sales", "orders", "id"),
            List.of("catalog", "sales", "ck_orders_total", "catalog", "sales", "orders", "total"),
            List.of(
                "catalog", "sales", "fk_orders_customer", "catalog", "sales", "customers", "id"))
        .doesNotContain(
            List.of(
                "catalog",
                "sales",
                "fk_orders_customer",
                "catalog",
                "sales",
                "orders",
                "customer_id"));
  }

  @Test
  void constraintTableUsage_includesLocalAndReferencedTables() {
    SystemObjectScanContext ctx = contextWithConstraints();

    var rows =
        new ConstraintTableUsageScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .contains(
            List.of("catalog", "sales", "fk_orders_customer", "catalog", "sales", "orders"),
            List.of("catalog", "sales", "fk_orders_customer", "catalog", "sales", "customers"));
  }

  @Test
  void tableSchemaName_rootNamespace() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var root = builder.addNamespace("sales");
    var ctx = contextWithSinglePkConstraint(builder, root, "orders");

    var rows = new TableConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();
    assertThat(rows)
        .contains(
            List.of(
                "catalog",
                "sales",
                "pk_orders",
                "catalog",
                "sales",
                "orders",
                "PRIMARY KEY",
                "NO",
                "NO",
                "YES"));
  }

  @Test
  void tableSchemaName_nestedNamespace() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var nested = builder.addNamespace("org.sales");
    var ctx = contextWithSinglePkConstraint(builder, nested, "orders");

    var rows = new TableConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();
    assertThat(rows)
        .contains(
            List.of(
                "catalog",
                "org.sales",
                "pk_orders",
                "catalog",
                "org.sales",
                "orders",
                "PRIMARY KEY",
                "NO",
                "NO",
                "YES"));
  }

  @Test
  void tableSchemaName_emptyDisplayName() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var namespace = builder.addNamespace(List.of("org", "sales"), "", "org.sales");
    var ctx = contextWithSinglePkConstraint(builder, namespace, "orders");

    var rows = new TableConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();
    assertThat(rows)
        .contains(
            List.of(
                "catalog",
                "org.sales",
                "pk_orders",
                "catalog",
                "org.sales",
                "orders",
                "PRIMARY KEY",
                "NO",
                "NO",
                "YES"));
  }

  @Test
  void tableSchemaName_pathAlreadyContainsLeaf() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var namespace = builder.addNamespace(List.of("org", "sales"), "sales", "org.sales");
    var ctx = contextWithSinglePkConstraint(builder, namespace, "orders");

    var rows = new TableConstraintsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();
    assertThat(rows)
        .contains(
            List.of(
                "catalog",
                "org.sales",
                "pk_orders",
                "catalog",
                "org.sales",
                "orders",
                "PRIMARY KEY",
                "NO",
                "NO",
                "YES"));
    assertThat(rows).noneMatch(r -> "org.sales.sales".equals(r.get(1)));
  }

  @Test
  void unresolvedReferencedTableDoesNotTriggerReferencedSchemaLookup() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var sales = builder.addNamespace("sales");
    var orders =
        builder.addTable(
            sales,
            "orders",
            Map.of("id", 1, "customer_id", 2),
            Map.of("id", "INT", "customer_id", "INT"));

    ConstraintDefinition fkUnresolved =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_missing_target")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(column("customer_id", 2L, 1))
            .setReferencedTable(
                NameRef.newBuilder()
                    .setCatalog("catalog")
                    .addPath("missing")
                    .setName("customers")
                    .build())
            .addReferencedColumns(column("id", 1L, 1))
            .build();

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(orders.id())) {
              return Optional.of(
                  new ConstraintSetView() {
                    @Override
                    public ResourceId relationId() {
                      return relationId;
                    }

                    @Override
                    public List<ConstraintDefinition> constraints() {
                      return List.of(fkUnresolved);
                    }
                  });
            }
            return Optional.empty();
          }
        };
    SystemObjectScanContext base = builder.build();
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            base.graph(),
            NameRef.getDefaultInstance(),
            base.queryDefaultCatalogId(),
            EngineContext.empty(),
            StatsProvider.NONE,
            provider);

    var rows =
        new ConstraintColumnUsageScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();
    assertThat(rows).isEmpty();
    TestCatalogOverlay overlay = (TestCatalogOverlay) ctx.graph();
    assertThat(overlay.tableSchemaLookupCountTotal()).isEqualTo(1);
    assertThat(overlay.tableSchemaLookupCount(orders.id())).isEqualTo(1);
  }

  @Test
  void repeatedFkReferencesToSameTargetResolveAndLoadTargetSchemaOnce() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var sales = builder.addNamespace("sales");
    var customers =
        builder.addTable(
            sales, "customers", Map.of("id", 1, "name", 2), Map.of("id", "INT", "name", "VARCHAR"));
    var orders =
        builder.addTable(
            sales,
            "orders",
            Map.of("id", 1, "customer_id", 2, "billing_customer_id", 3),
            Map.of("id", "INT", "customer_id", "INT", "billing_customer_id", "INT"));

    ConstraintDefinition fkCustomer =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customer")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(column("customer_id", 2L, 1))
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .addReferencedColumns(column("id", 1L, 1))
            .build();
    ConstraintDefinition fkBillingCustomer =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_billing_customer")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(column("billing_customer_id", 3L, 1))
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .addReferencedColumns(column("id", 1L, 1))
            .build();

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(orders.id())) {
              return Optional.of(
                  new ConstraintSetView() {
                    @Override
                    public ResourceId relationId() {
                      return relationId;
                    }

                    @Override
                    public List<ConstraintDefinition> constraints() {
                      return List.of(fkCustomer, fkBillingCustomer);
                    }
                  });
            }
            return Optional.empty();
          }
        };

    SystemObjectScanContext base = builder.build();
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            base.graph(),
            NameRef.getDefaultInstance(),
            base.queryDefaultCatalogId(),
            EngineContext.empty(),
            StatsProvider.NONE,
            provider);

    var rows =
        new ConstraintColumnUsageScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();
    assertThat(rows).hasSize(2);

    TestCatalogOverlay overlay = (TestCatalogOverlay) ctx.graph();
    assertThat(overlay.tableSchemaLookupCount(orders.id())).isEqualTo(1);
    assertThat(overlay.tableSchemaLookupCount(customers.id())).isEqualTo(1);
    assertThat(overlay.resolveTableLookupCountTotal()).isEqualTo(1);
  }

  private static SystemObjectScanContext contextWithConstraints() {
    return contextWithConstraints(null);
  }

  @Test
  void scannersReuseConstraintIndexPerContext() {
    AtomicInteger providerCalls = new AtomicInteger();
    SystemObjectScanContext ctx = contextWithConstraints(providerCalls);

    new TableConstraintsScanner().scan(ctx).toList();
    new KeyColumnUsageScanner().scan(ctx).toList();
    new CheckConstraintsScanner().scan(ctx).toList();

    assertThat(providerCalls.get())
        .isEqualTo(2); // one lookup per visible table, once per shared index build
  }

  private static SystemObjectScanContext contextWithConstraints(AtomicInteger providerCalls) {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var sales = builder.addNamespace("sales");
    var orders =
        builder.addTable(
            sales,
            "orders",
            Map.of("id", 1, "customer_id", 2, "total", 3),
            Map.of("id", "INT", "customer_id", "INT", "total", "INT"));
    var customers =
        builder.addTable(
            sales, "customers", Map.of("id", 1, "name", 2), Map.of("id", "INT", "name", "VARCHAR"));

    ConstraintDefinition pk =
        ConstraintDefinition.newBuilder()
            .setName("pk_orders")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .addColumns(column("id", 1L, 1))
            .build();
    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customer")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setEnforcement(ConstraintEnforcement.CE_NOT_ENFORCED)
            .addColumns(column("customer_id", 2L, 1))
            .setReferencedTableId(customers.id())
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .setReferencedConstraintName("pk_customers")
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT)
            .addReferencedColumns(column("id", 1L, 1))
            .build();
    ConstraintDefinition check =
        ConstraintDefinition.newBuilder()
            .setName("ck_orders_total")
            .setType(ConstraintType.CT_CHECK)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .setCheckExpression("total >= 0")
            .addColumns(column("total", 3L, 1))
            .build();

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (providerCalls != null) {
              providerCalls.incrementAndGet();
            }
            if (relationId.equals(orders.id())) {
              return Optional.of(
                  new ConstraintProvider.ConstraintSetView() {
                    @Override
                    public ResourceId relationId() {
                      return relationId;
                    }

                    @Override
                    public List<ConstraintDefinition> constraints() {
                      return List.of(pk, fk, check);
                    }
                  });
            }
            return Optional.empty();
          }
        };

    SystemObjectScanContext base = builder.build();
    return new SystemObjectScanContext(
        base.graph(),
        NameRef.getDefaultInstance(),
        base.queryDefaultCatalogId(),
        EngineContext.empty(),
        StatsProvider.NONE,
        provider);
  }

  private static SystemObjectScanContext contextWithSinglePkConstraint(
      TestTableScanContextBuilder builder,
      ai.floedb.floecat.metagraph.model.NamespaceNode namespace,
      String tableName) {
    var table = builder.addTable(namespace, tableName, Map.of("id", 1), Map.of("id", "INT"));
    ConstraintDefinition pk =
        ConstraintDefinition.newBuilder()
            .setName("pk_" + tableName)
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .addColumns(column("id", 1L, 1))
            .build();
    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotIdArg) {
            if (relationId.equals(table.id())) {
              return Optional.of(
                  new ConstraintSetView() {
                    @Override
                    public ResourceId relationId() {
                      return relationId;
                    }

                    @Override
                    public List<ConstraintDefinition> constraints() {
                      return List.of(pk);
                    }
                  });
            }
            return Optional.empty();
          }
        };
    SystemObjectScanContext base = builder.build();
    return new SystemObjectScanContext(
        base.graph(),
        NameRef.getDefaultInstance(),
        base.queryDefaultCatalogId(),
        EngineContext.empty(),
        StatsProvider.NONE,
        provider);
  }

  private static ConstraintColumnRef column(String name, long id, int ordinal) {
    return ConstraintColumnRef.newBuilder()
        .setColumnName(name)
        .setColumnId(id)
        .setOrdinal(ordinal)
        .build();
  }

  private static SystemObjectScanContext contextWithOnlyNotNullConstraint() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var sales = builder.addNamespace("sales");
    var orders =
        builder.addTable(sales, "orders", Map.of("id", 1, "v", 2), Map.of("id", "INT", "v", "INT"));

    ConstraintDefinition notNull =
        ConstraintDefinition.newBuilder()
            .setName("nn_orders_v")
            .setType(ConstraintType.CT_NOT_NULL)
            .addColumns(column("v", 2L, 1))
            .build();

    ConstraintProvider provider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId relationId, OptionalLong snapshotId) {
            if (relationId.equals(orders.id())) {
              return Optional.of(
                  new ConstraintSetView() {
                    @Override
                    public ResourceId relationId() {
                      return relationId;
                    }

                    @Override
                    public List<ConstraintDefinition> constraints() {
                      return List.of(notNull);
                    }
                  });
            }
            return Optional.empty();
          }
        };

    SystemObjectScanContext base = builder.build();
    return new SystemObjectScanContext(
        base.graph(),
        NameRef.getDefaultInstance(),
        base.queryDefaultCatalogId(),
        EngineContext.empty(),
        StatsProvider.NONE,
        provider);
  }
}
