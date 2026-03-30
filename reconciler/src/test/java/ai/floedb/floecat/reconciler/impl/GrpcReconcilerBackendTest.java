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
package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import org.junit.jupiter.api.Test;

class GrpcReconcilerBackendTest {
  @Test
  void withBearerPrefixLeavesExistingBearer() {
    String token = GrpcReconcilerBackend.withBearerPrefix("Bearer abc123");
    assertThat(token).isEqualTo("Bearer abc123");
  }

  @Test
  void withBearerPrefixAddsPrefixWhenMissing() {
    String token = GrpcReconcilerBackend.withBearerPrefix("abc123");
    assertThat(token).isEqualTo("Bearer abc123");
  }

  @Test
  void snapshotConstraintsIdempotencyKeyIsStableForSamePayload() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("users")
            .build();
    SnapshotConstraints constraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();

    PutTableConstraintsRequest request1 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, constraints);
    PutTableConstraintsRequest request2 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, constraints);

    assertThat(request1.getIdempotency().getKey()).isEqualTo(request2.getIdempotency().getKey());
  }

  @Test
  void snapshotConstraintsIdempotencyKeyChangesWhenPayloadChanges() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("users")
            .build();
    SnapshotConstraints first =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();
    SnapshotConstraints second =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users_alt")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();

    PutTableConstraintsRequest request1 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, first);
    PutTableConstraintsRequest request2 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, second);

    assertThat(request1.getIdempotency().getKey()).isNotEqualTo(request2.getIdempotency().getKey());
  }

  @Test
  void buildPutTableConstraintsRequestPopulatesAllFieldsAndStableIdempotencyKey() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("users")
            .build();
    SnapshotConstraints snapshotConstraints =
        SnapshotConstraints.newBuilder()
            .addConstraints(
                ConstraintDefinition.newBuilder()
                    .setName("pk_users")
                    .setType(ConstraintType.CT_PRIMARY_KEY)
                    .build())
            .build();

    PutTableConstraintsRequest request =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, snapshotConstraints);
    PutTableConstraintsRequest request2 =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 42L, snapshotConstraints);

    assertThat(request.getTableId()).isEqualTo(tableId);
    assertThat(request.getSnapshotId()).isEqualTo(42L);
    assertThat(request.getConstraints()).isEqualTo(snapshotConstraints);
    assertThat(request.getIdempotency().getKey()).isNotBlank();
    assertThat(request2.getIdempotency().getKey()).isEqualTo(request.getIdempotency().getKey());
  }

  @Test
  void buildPutTableConstraintsRequestPreservesForeignKeyMetadataFields() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();
    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customers")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .setReferencedConstraintName("pk_customers")
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT)
            .build();
    SnapshotConstraints snapshotConstraints =
        SnapshotConstraints.newBuilder().addConstraints(fk).build();

    PutTableConstraintsRequest request =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 77L, snapshotConstraints);
    ConstraintDefinition emitted = request.getConstraints().getConstraints(0);

    assertThat(emitted.getReferencedConstraintName()).isEqualTo("pk_customers");
    assertThat(emitted.getMatchOption()).isEqualTo(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL);
    assertThat(emitted.getUpdateRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE);
    assertThat(emitted.getDeleteRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT);
  }

  @Test
  void buildPutTableConstraintsRequestLeavesForeignKeyRulesUnspecifiedWhenNotProvided() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();
    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_orders_customers_default")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setReferencedTable(NameRef.newBuilder().addPath("sales").setName("customers").build())
            .build();
    SnapshotConstraints snapshotConstraints =
        SnapshotConstraints.newBuilder().addConstraints(fk).build();

    PutTableConstraintsRequest request =
        GrpcReconcilerBackend.buildPutTableConstraintsRequest(tableId, 78L, snapshotConstraints);
    ConstraintDefinition emitted = request.getConstraints().getConstraints(0);

    assertThat(emitted.getMatchOption())
        .isEqualTo(ForeignKeyMatchOption.FK_MATCH_OPTION_UNSPECIFIED);
    assertThat(emitted.getUpdateRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED);
    assertThat(emitted.getDeleteRule()).isEqualTo(ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED);
  }

  @Test
  void buildStatsAlreadyCapturedRequestIncludesTableId() {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("orders")
            .build();

    GetTargetStatsRequest request =
        GrpcReconcilerBackend.buildStatsAlreadyCapturedRequest(tableId, 99L);

    assertThat(request.getTableId()).isEqualTo(tableId);
    assertThat(request.getSnapshot().getSnapshotId()).isEqualTo(99L);
    assertThat(request.getTarget().hasTable()).isTrue();
  }
}
