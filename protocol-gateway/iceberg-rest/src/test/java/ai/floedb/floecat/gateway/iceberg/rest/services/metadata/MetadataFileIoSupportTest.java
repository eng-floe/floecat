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

package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MetadataFileIoSupportTest {

  @Test
  void materializationIoPropertiesUsesLocationScopedServerSideConfigWhenTablePresent() {
    MetadataFileIoSupport support = new MetadataFileIoSupport();
    TableGatewaySupport tableGatewaySupport = mock(TableGatewaySupport.class);
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setKind(ResourceKind.RK_TABLE).setId("tbl-1"))
            .build();
    support.tableGatewaySupport = tableGatewaySupport;

    when(tableGatewaySupport.serverSideFileIoPropertiesForLocation(table, "s3://warehouse/orders"))
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localstack:4566",
                "s3.path-style-access", "true"));

    Map<String, String> props =
        support.materializationIoProperties(
            table, Map.of("location", "s3://warehouse/orders", "s3.region", "us-east-1"), true);

    assertEquals("http://localstack:4566", props.get("s3.endpoint"));
    assertEquals("true", props.get("s3.path-style-access"));
    assertEquals("us-east-1", props.get("s3.region"));
  }

  @Test
  void materializationIoPropertiesUsesLocationOnlyForUnpersistedTableStub() {
    MetadataFileIoSupport support = new MetadataFileIoSupport();
    TableGatewaySupport tableGatewaySupport = mock(TableGatewaySupport.class);
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setKind(ResourceKind.RK_NAMESPACE).setId("ns"))
            .build();
    support.tableGatewaySupport = tableGatewaySupport;

    when(tableGatewaySupport.serverSideFileIoPropertiesForLocation("s3://warehouse/orders"))
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localstack:4566",
                "s3.path-style-access", "true"));

    Map<String, String> props =
        support.materializationIoProperties(
            table, Map.of("location", "s3://warehouse/orders", "s3.region", "us-east-1"), false);

    assertEquals("http://localstack:4566", props.get("s3.endpoint"));
    assertEquals("true", props.get("s3.path-style-access"));
    assertEquals("us-east-1", props.get("s3.region"));
    verify(tableGatewaySupport).serverSideFileIoPropertiesForLocation("s3://warehouse/orders");
    verify(tableGatewaySupport, never())
        .serverSideFileIoPropertiesForLocation(table, "s3://warehouse/orders");
  }

  @Test
  void materializationIoPropertiesUsesLocationOnlyWhenTableScopedIoIsDisabled() {
    MetadataFileIoSupport support = new MetadataFileIoSupport();
    TableGatewaySupport tableGatewaySupport = mock(TableGatewaySupport.class);
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setKind(ResourceKind.RK_TABLE).setId("tbl-1"))
            .build();
    support.tableGatewaySupport = tableGatewaySupport;

    when(tableGatewaySupport.serverSideFileIoPropertiesForLocation("s3://warehouse/orders"))
        .thenReturn(
            Map.of(
                "s3.endpoint", "http://localstack:4566",
                "s3.path-style-access", "true"));

    Map<String, String> props =
        support.materializationIoProperties(
            table, Map.of("location", "s3://warehouse/orders", "s3.region", "us-east-1"), false);

    assertEquals("http://localstack:4566", props.get("s3.endpoint"));
    assertEquals("true", props.get("s3.path-style-access"));
    assertEquals("us-east-1", props.get("s3.region"));
    verify(tableGatewaySupport).serverSideFileIoPropertiesForLocation("s3://warehouse/orders");
    verify(tableGatewaySupport, never())
        .serverSideFileIoPropertiesForLocation(table, "s3://warehouse/orders");
  }
}
