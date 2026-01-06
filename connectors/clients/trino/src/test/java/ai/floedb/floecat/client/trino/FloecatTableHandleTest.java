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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.ResourceKind;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FloecatTableHandleTest {

  @Test
  void toIcebergTableHandlePreservesImportantFields() {
    IcebergColumnHandle col =
        new IcebergColumnHandle(
            ColumnIdentity.primitiveColumnIdentity(1, "id"),
            BigintType.BIGINT,
            java.util.List.of(),
            BigintType.BIGINT,
            false,
            java.util.Optional.empty());
    TupleDomain<IcebergColumnHandle> domain =
        TupleDomain.withColumnDomains(Map.of(col, Domain.singleValue(BigintType.BIGINT, 1L)));

    FloecatTableHandle handle =
        new FloecatTableHandle(
            new SchemaTableName("s", "t"),
            "tbl-id",
            "account",
            ResourceKind.RK_TABLE.name(),
            "s3://bucket/table",
            "{}",
            "{}",
            "TF_ICEBERG",
            "test:normal:v1",
            domain,
            Set.of("id"),
            null,
            null);

    IcebergTableHandle icebergHandle = handle.toIcebergTableHandle(Map.of("k", "v"));

    assertEquals("s", icebergHandle.getSchemaName());
    assertEquals("t", icebergHandle.getTableName());
    assertEquals("{}", icebergHandle.getTableSchemaJson());
    assertEquals(java.util.Optional.of("{}"), icebergHandle.getPartitionSpecJson());
    assertEquals(domain, icebergHandle.getUnenforcedPredicate());
    assertEquals(TupleDomain.all(), icebergHandle.getEnforcedPredicate());
    assertEquals(Map.of("k", "v"), icebergHandle.getStorageProperties());
  }

  @Test
  void getTableResourceIdRequiresAccount() {
    FloecatTableHandle handle =
        new FloecatTableHandle(
            new SchemaTableName("s", "t"),
            "tbl-id",
            "",
            ResourceKind.RK_TABLE.name(),
            "s3://bucket/table",
            "{}",
            "{}",
            "TF_ICEBERG",
            "catalog",
            TupleDomain.all(),
            Set.of(),
            null,
            null);

    assertThrows(IllegalStateException.class, handle::getTableResourceId);
  }
}
