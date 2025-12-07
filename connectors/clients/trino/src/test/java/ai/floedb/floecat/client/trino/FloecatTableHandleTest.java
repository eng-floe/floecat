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
