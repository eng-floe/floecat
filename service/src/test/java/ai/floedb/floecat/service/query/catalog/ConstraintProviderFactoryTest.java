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

package ai.floedb.floecat.service.query.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ConstraintProviderFactoryTest {

  private static final ResourceId USER_TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("users")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final ResourceId SYSTEM_TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("information_schema.tables")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  @Test
  void userConstraintsReadFromRepositoryWhenSnapshotProvided() {
    CountingConstraintRepository repository = new CountingConstraintRepository();
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    TableRepository tables = new TableRepository(pointers, blobs);
    SnapshotRepository snapshots = new SnapshotRepository(pointers, blobs, tables);
    CatalogOverlay overlay = mock(CatalogOverlay.class);
    when(overlay.resolve(USER_TABLE)).thenReturn(Optional.empty());

    ConstraintProviderFactory factory =
        ConstraintProviderFactory.forTesting(
            repository, snapshots, overlay, ConstraintProvider.NONE);
    long snapshotId = 101L;
    repository.putSnapshotConstraints(
        USER_TABLE, snapshotId, constraints(USER_TABLE, snapshotId, "pk_users"));

    ConstraintProvider provider = factory.provider();

    var view = provider.constraints(USER_TABLE, OptionalLong.of(snapshotId)).orElseThrow();
    assertEquals(USER_TABLE, view.relationId());
    assertEquals("pk_users", view.constraints().get(0).getName());
    assertEquals(1, repository.getCalls());
  }

  @Test
  void missingSnapshotReturnsNoConstraints() {
    CountingConstraintRepository repository = new CountingConstraintRepository();
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    TableRepository tables = new TableRepository(pointers, blobs);
    SnapshotRepository snapshots = new SnapshotRepository(pointers, blobs, tables);
    CatalogOverlay overlay = mock(CatalogOverlay.class);
    when(overlay.resolve(USER_TABLE)).thenReturn(Optional.empty());
    ConstraintProviderFactory factory =
        ConstraintProviderFactory.forTesting(
            repository, snapshots, overlay, ConstraintProvider.NONE);
    ConstraintProvider provider = factory.provider();

    assertTrue(provider.constraints(USER_TABLE, OptionalLong.empty()).isEmpty());
    assertEquals(0, repository.getCalls());
  }

  @Test
  void latestConstraintsUsesCurrentSnapshotForUserTable() {
    CountingConstraintRepository repository = new CountingConstraintRepository();
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    TableRepository tables = new TableRepository(pointers, blobs);
    SnapshotRepository snapshots = new SnapshotRepository(pointers, blobs, tables);
    CatalogOverlay overlay = mock(CatalogOverlay.class);
    when(overlay.resolve(USER_TABLE)).thenReturn(Optional.empty());

    ConstraintProviderFactory factory =
        ConstraintProviderFactory.forTesting(
            repository, snapshots, overlay, ConstraintProvider.NONE);
    repository.putSnapshotConstraints(USER_TABLE, 100L, constraints(USER_TABLE, 100L, "pk_v100"));
    repository.putSnapshotConstraints(USER_TABLE, 200L, constraints(USER_TABLE, 200L, "pk_v200"));
    tables.create(
        ai.floedb.floecat.catalog.rpc.Table.newBuilder()
            .setResourceId(USER_TABLE)
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("catalog")
                    .setKind(ResourceKind.RK_CATALOG)
                    .build())
            .setNamespaceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setId("namespace")
                    .setKind(ResourceKind.RK_NAMESPACE)
                    .build())
            .setDisplayName("users")
            .putProperties("current-snapshot-id", "200")
            .build());
    snapshots.create(snapshot(USER_TABLE, 100L, 1_000L, 1_000L));
    snapshots.create(snapshot(USER_TABLE, 200L, 2_000L, 2_000L));

    ConstraintProvider provider = factory.provider();
    var latest = provider.latestConstraints(USER_TABLE).orElseThrow();
    assertEquals("pk_v200", latest.constraints().get(0).getName());
  }

  @Test
  void routesSystemRelationsToSystemProvider() {
    CountingConstraintRepository repository = new CountingConstraintRepository();
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();
    TableRepository tables = new TableRepository(pointers, blobs);
    SnapshotRepository snapshots = new SnapshotRepository(pointers, blobs, tables);
    CatalogOverlay overlay = mock(CatalogOverlay.class);
    GraphNode systemNode = mock(GraphNode.class);
    when(systemNode.origin()).thenReturn(GraphNodeOrigin.SYSTEM);
    when(systemNode.kind()).thenReturn(GraphNodeKind.TABLE);
    when(systemNode.id()).thenReturn(SYSTEM_TABLE);
    when(overlay.resolve(SYSTEM_TABLE)).thenReturn(Optional.of(systemNode));
    when(overlay.resolve(USER_TABLE)).thenReturn(Optional.empty());

    long snapshotId = 202L;
    repository.putSnapshotConstraints(
        USER_TABLE, snapshotId, constraints(USER_TABLE, snapshotId, "repo_users"));
    repository.putSnapshotConstraints(
        SYSTEM_TABLE, snapshotId, constraints(SYSTEM_TABLE, snapshotId, "repo_system"));

    ConstraintProvider systemProvider =
        new ConstraintProvider() {
          @Override
          public Optional<ConstraintSetView> constraints(
              ResourceId tableId, OptionalLong requestedSnapshotId) {
            if (!tableId.equals(SYSTEM_TABLE)) {
              return Optional.empty();
            }
            assertTrue(requestedSnapshotId.isEmpty());
            return Optional.of(
                ConstraintProviderFactory.constraintSetView(
                    ConstraintProviderFactoryTest.constraints(
                        SYSTEM_TABLE, snapshotId, "system_static")));
          }
        };

    ConstraintProviderFactory factory =
        ConstraintProviderFactory.forTesting(repository, snapshots, overlay, systemProvider);
    ConstraintProvider provider = factory.provider();

    var system = provider.constraints(SYSTEM_TABLE, OptionalLong.of(snapshotId)).orElseThrow();
    assertEquals("system_static", system.constraints().get(0).getName());

    var user = provider.constraints(USER_TABLE, OptionalLong.of(snapshotId)).orElseThrow();
    assertEquals("repo_users", user.constraints().get(0).getName());
    assertEquals(0, repository.systemGetCalls());
    assertEquals(1, repository.userGetCalls());
  }

  private static SnapshotConstraints constraints(ResourceId tableId, long snapshotId, String name) {
    return SnapshotConstraints.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .addConstraints(
            ConstraintDefinition.newBuilder()
                .setName(name)
                .setType(ConstraintType.CT_PRIMARY_KEY)
                .addColumns(
                    ConstraintColumnRef.newBuilder()
                        .setOrdinal(1)
                        .setColumnId(1L)
                        .setColumnName("id")
                        .build())
                .build())
        .build();
  }

  private static Snapshot snapshot(
      ResourceId tableId, long snapshotId, long ingestedAtMs, long upstreamCreatedAtMs) {
    return Snapshot.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setIngestedAt(Timestamps.fromMillis(ingestedAtMs))
        .setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreatedAtMs))
        .build();
  }

  private static final class CountingConstraintRepository extends ConstraintRepository {

    private int getCalls;
    private int userGetCalls;
    private int systemGetCalls;

    private CountingConstraintRepository() {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    @Override
    public Optional<SnapshotConstraints> getSnapshotConstraints(
        ResourceId tableId, long snapshotId) {
      getCalls++;
      if (SYSTEM_TABLE.equals(tableId)) {
        systemGetCalls++;
      } else if (USER_TABLE.equals(tableId)) {
        userGetCalls++;
      }
      return super.getSnapshotConstraints(tableId, snapshotId);
    }

    private int getCalls() {
      return getCalls;
    }

    private int userGetCalls() {
      return userGetCalls;
    }

    private int systemGetCalls() {
      return systemGetCalls;
    }
  }
}
