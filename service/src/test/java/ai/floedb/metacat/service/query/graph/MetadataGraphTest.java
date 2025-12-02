package ai.floedb.metacat.service.query.graph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.repo.impl.ViewRepository;
import ai.floedb.metacat.storage.errors.StorageNotFoundException;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MetadataGraphTest {

  @Mock CatalogRepository catalogRepository;
  @Mock NamespaceRepository namespaceRepository;
  @Mock TableRepository tableRepository;
  @Mock ViewRepository viewRepository;

  MetadataGraph graph;

  @BeforeEach
  void setUp() {
    reset(catalogRepository, namespaceRepository, tableRepository, viewRepository);
    graph =
        new MetadataGraph(catalogRepository, namespaceRepository, tableRepository, viewRepository);
  }

  @Test
  void tableResolveCachesNodes() {
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("tenant", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("tenant", "tbl", ResourceKind.RK_TABLE);

    MutationMeta meta = mutationMeta(7L, Instant.parse("2024-01-01T00:00:00Z"));
    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("sales")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_ICEBERG)
                    .setUri("s3://bucket/sales")
                    .build())
            .build();

    when(tableRepository.metaForSafe(tableId)).thenReturn(meta);
    when(tableRepository.getById(tableId)).thenReturn(Optional.of(table));

    Optional<TableNode> first = graph.table(tableId);
    Optional<TableNode> second = graph.table(tableId);

    assertThat(first).isPresent();
    assertThat(second).containsSame(first.get());
    verify(tableRepository, times(1)).getById(tableId);

    graph.invalidate(tableId);
    Optional<TableNode> third = graph.table(tableId);
    assertThat(third).isPresent();
    verify(tableRepository, times(2)).getById(tableId);
  }

  @Test
  void catalogWrapperReturnsNode() {
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    MutationMeta meta = mutationMeta(3L, Instant.parse("2023-06-01T00:00:00Z"));
    Catalog catalog =
        Catalog.newBuilder()
            .setResourceId(catalogId)
            .setDisplayName("analytics")
            .putProperties("env", "dev")
            .build();

    when(catalogRepository.metaForSafe(catalogId)).thenReturn(meta);
    when(catalogRepository.getById(catalogId)).thenReturn(Optional.of(catalog));

    Optional<CatalogNode> node = graph.catalog(catalogId);
    assertThat(node).isPresent();
    assertThat(node.get().displayName()).isEqualTo("analytics");
    verify(catalogRepository, times(1)).getById(catalogId);
  }

  @Test
  void namespaceWrapperReturnsNode() {
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("tenant", "ns", ResourceKind.RK_NAMESPACE);
    MutationMeta meta = mutationMeta(11L, Instant.parse("2024-03-10T10:15:30Z"));
    Namespace namespace =
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("finance")
            .addParents("root")
            .putProperties("region", "us-west-2")
            .build();

    when(namespaceRepository.metaForSafe(namespaceId)).thenReturn(meta);
    when(namespaceRepository.getById(namespaceId)).thenReturn(Optional.of(namespace));

    Optional<NamespaceNode> node = graph.namespace(namespaceId);
    assertThat(node).isPresent();
    assertThat(node.get().catalogId()).isEqualTo(catalogId);
    assertThat(node.get().pathSegments()).containsExactly("root");
  }

  @Test
  void viewWrapperReturnsNode() {
    ResourceId catalogId = rid("tenant", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("tenant", "ns", ResourceKind.RK_NAMESPACE);
    ResourceId viewId = rid("tenant", "view", ResourceKind.RK_VIEW);
    MutationMeta meta = mutationMeta(15L, Instant.parse("2024-05-01T05:06:07Z"));
    View view =
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("sales_view")
            .setSql("select * from sales")
            .build();

    when(viewRepository.metaForSafe(viewId)).thenReturn(meta);
    when(viewRepository.getById(viewId)).thenReturn(Optional.of(view));

    Optional<ViewNode> node = graph.view(viewId);
    assertThat(node).isPresent();
    assertThat(node.get().sql()).contains("sales");
  }

  @Test
  void resolveReturnsEmptyWhenMetadataMissing() {
    ResourceId missingId = rid("tenant", "missing", ResourceKind.RK_CATALOG);
    when(catalogRepository.metaForSafe(missingId))
        .thenThrow(new StorageNotFoundException("not found"));

    Optional<CatalogNode> node = graph.catalog(missingId);
    assertThat(node).isEmpty();
  }

  @Test
  void resolveReturnsEmptyWhenRepoHasNoResource() {
    ResourceId viewId = rid("tenant", "view", ResourceKind.RK_VIEW);
    MutationMeta meta = mutationMeta(1L, Instant.parse("2024-02-01T00:00:00Z"));
    when(viewRepository.metaForSafe(viewId)).thenReturn(meta);
    when(viewRepository.getById(viewId)).thenReturn(Optional.empty());

    Optional<ViewNode> node = graph.view(viewId);
    assertThat(node).isEmpty();
  }

  private static MutationMeta mutationMeta(long version, Instant updatedAt) {
    Timestamp ts =
        Timestamp.newBuilder()
            .setSeconds(updatedAt.getEpochSecond())
            .setNanos(updatedAt.getNano())
            .build();
    return MutationMeta.newBuilder().setPointerVersion(version).setUpdatedAt(ts).build();
  }

  private static ResourceId rid(String tenant, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setTenantId(tenant).setId(id).setKind(kind).build();
  }
}
