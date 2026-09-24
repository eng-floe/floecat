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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorNotReadyException;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

class IcebergConnectorIncrementalEnumerationTest {

  @Test
  void snapshotsToEnumerateIncludesUnknownSnapshotsOutsideCurrentLineage() throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };

    Snapshot mainHead = snapshot(300L, 3L, 3000L, 200L);
    Snapshot mainParent = snapshot(200L, 2L, 2000L, 100L);
    Snapshot known = snapshot(100L, 1L, 1000L, null);
    Snapshot branchOnly = snapshot(250L, 4L, 2500L, 150L);

    Table table = table(List.of(mainHead, branchOnly, mainParent, known), mainHead);

    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots =
        (Stream<Snapshot>)
            method.invoke(
                connector,
                table,
                false,
                Set.of(100L, 200L),
                Set.of(),
                FloecatConnector.SnapshotSelectionKind.ALL,
                Set.of(),
                0);

    assertEquals(List.of(300L, 250L), snapshots.map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumerateFullRescanReturnsAllSnapshots() throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };

    Snapshot latest = snapshot(300L, 3L, 3000L, 200L);
    Snapshot target = snapshot(200L, 2L, 2000L, 100L);
    Snapshot earlier = snapshot(100L, 1L, 1000L, null);
    Table table = table(List.of(latest, earlier, target), latest);

    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots =
        (Stream<Snapshot>)
            method.invoke(
                connector,
                table,
                true,
                Set.of(),
                Set.of(),
                FloecatConnector.SnapshotSelectionKind.ALL,
                Set.of(),
                0);

    assertEquals(List.of(300L, 100L, 200L), snapshots.map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumeratePreservesMetadataOrderInsteadOfSortingBySequenceOrSnapshotId()
      throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };
    Snapshot first = snapshot(17L, 5L, 5000L, null);
    Snapshot second = snapshot(900_000_007L, 2L, 2000L, first.snapshotId());
    Table table = table(List.of(first, second), second);
    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots =
        (Stream<Snapshot>)
            method.invoke(
                connector,
                table,
                true,
                Set.of(),
                Set.of(),
                FloecatConnector.SnapshotSelectionKind.ALL,
                Set.of(),
                0);

    assertEquals(List.of(17L, 900_000_007L), snapshots.map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumerateLatestNUsesSequenceNumberNotMetadataOrSnapshotIdOrder()
      throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };
    Snapshot newest = snapshot(11L, 9L, 9000L, null);
    Snapshot oldest = snapshot(999_999_937L, 1L, 1000L, null);
    Snapshot middle = snapshot(23L, 5L, 5000L, null);
    Table table = table(List.of(newest, oldest, middle), newest);
    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots =
        (Stream<Snapshot>)
            method.invoke(
                connector,
                table,
                true,
                Set.of(),
                Set.of(),
                FloecatConnector.SnapshotSelectionKind.LATEST_N,
                Set.of(),
                2);

    assertEquals(List.of(23L, 11L), snapshots.map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumerateLatestNBreaksTiesByMetadataPositionNotSnapshotId() throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };
    // v1 tables report sequenceNumber 0 for every snapshot; same-millisecond commits then tie on
    // both ordering keys, and snapshot ids are random so they cannot break the tie meaningfully.
    Snapshot first = snapshot(900_000_007L, 0L, 1000L, null);
    Snapshot second = snapshot(11L, 0L, 1000L, first.snapshotId());
    Snapshot third = snapshot(500_000_003L, 0L, 1000L, second.snapshotId());
    Table table = table(List.of(first, second, third), third);
    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots =
        (Stream<Snapshot>)
            method.invoke(
                connector,
                table,
                true,
                Set.of(),
                Set.of(),
                FloecatConnector.SnapshotSelectionKind.LATEST_N,
                Set.of(),
                2);

    // The two latest by history position, in history order -- not the two largest ids.
    assertEquals(List.of(11L, 500_000_003L), snapshots.map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumerateLatestNDoesNotPreallocateForUnboundedLatestN() throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };
    Snapshot newest = snapshot(11L, 3L, 3000L, null);
    Snapshot middle = snapshot(23L, 2L, 2000L, null);
    Snapshot oldest = snapshot(37L, 1L, 1000L, null);
    Table table = table(List.of(newest, middle, oldest), newest);
    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    // latest_n is an unvalidated uint32 on the wire; a policy value this large must not drive an
    // up-front array allocation on a table that only has three snapshots.
    Object result;
    try {
      result =
          method.invoke(
              connector,
              table,
              true,
              Set.of(),
              Set.of(),
              FloecatConnector.SnapshotSelectionKind.LATEST_N,
              Set.of(),
              Integer.MAX_VALUE);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof OutOfMemoryError oom) {
        throw new AssertionError(
            "LATEST_N preallocated from the unvalidated latest_n policy value", oom);
      }
      throw e;
    }

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots = (Stream<Snapshot>) result;
    assertEquals(List.of(37L, 23L, 11L), snapshots.map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumerateConsumesAllSelectionLazily() throws Exception {
    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            throw new UnsupportedOperationException();
          }
        };
    Snapshot first = snapshot(71L, 1L, 1000L, null);
    Snapshot second = snapshot(23L, 2L, 2000L, first.snapshotId());
    AtomicInteger consumed = new AtomicInteger();
    Iterable<Snapshot> source =
        () ->
            List.of(first, second).stream().peek(ignored -> consumed.incrementAndGet()).iterator();
    Table table = table(source, second);
    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate",
            Table.class,
            boolean.class,
            Set.class,
            Set.class,
            FloecatConnector.SnapshotSelectionKind.class,
            Set.class,
            int.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    Stream<Snapshot> snapshots =
        (Stream<Snapshot>)
            method.invoke(
                connector,
                table,
                true,
                Set.of(),
                Set.of(),
                FloecatConnector.SnapshotSelectionKind.ALL,
                Set.of(),
                0);

    assertEquals(0, consumed.get());
    assertEquals(71L, snapshots.findFirst().orElseThrow().snapshotId());
    assertEquals(1, consumed.get());
  }

  @Test
  void enumerateSnapshotsFailsWhenCurrentSnapshotExistsButIncrementalEnumerationIsEmpty() {
    Snapshot current = snapshot(300L, 3L, 3000L, 200L);
    Table table = table(List.of(), current);

    IcebergConnector connector =
        new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
          @Override
          public List<String> listNamespaces() {
            return List.of();
          }

          @Override
          public List<String> listTables(String namespaceFq) {
            return List.of();
          }

          @Override
          protected Table loadTableFromSource(String namespaceFq, String tableName) {
            return table;
          }
        };

    assertThrows(
        ConnectorNotReadyException.class,
        () ->
            connector
                .enumerateSnapshots(
                    "iceberg",
                    "duckdb_mutation_smoke",
                    ResourceId.getDefaultInstance(),
                    FloecatConnector.SnapshotEnumerationOptions.incremental(Set.of()))
                .toList());
  }

  private static Snapshot snapshot(
      long snapshotId, long sequenceNumber, long timestampMillis, Long parentSnapshotId) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "snapshotId" -> snapshotId;
            case "sequenceNumber" -> sequenceNumber;
            case "timestampMillis" -> timestampMillis;
            case "parentId" -> parentSnapshotId;
            case "schemaId" -> null;
            case "summary" -> Map.of();
            case "manifestListLocation" -> null;
            case "operation" -> null;
            case "allManifests" -> List.of();
            case "dataManifests" -> List.of();
            case "deleteManifests" -> List.of();
            case "addedDataFiles" -> null;
            case "removedDataFiles" -> null;
            case "addedDeleteFiles" -> null;
            case "removedDeleteFiles" -> null;
            default -> defaultValue(method);
          };
        };
    return (Snapshot)
        Proxy.newProxyInstance(
            Snapshot.class.getClassLoader(), new Class<?>[] {Snapshot.class}, handler);
  }

  private static Table table(List<Snapshot> snapshots, Snapshot current) {
    return table((Iterable<Snapshot>) snapshots, current);
  }

  private static Table table(Iterable<Snapshot> snapshots, Snapshot current) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "snapshots" -> snapshots;
            case "currentSnapshot" -> current;
            case "snapshot" -> {
              Long id = (Long) args[0];
              Snapshot match = null;
              for (Snapshot snapshot : snapshots) {
                if (snapshot.snapshotId() == id) {
                  match = snapshot;
                  break;
                }
              }
              yield match;
            }
            case "toString" -> "table";
            case "name" -> "test";
            case "location" -> "s3://test";
            case "properties", "schemas", "sortOrders", "refs" -> Map.of();
            case "history" -> List.of();
            default -> defaultValue(method);
          };
        };
    return (Table)
        Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class}, handler);
  }

  private static Object defaultValue(Method method) {
    Class<?> type = method.getReturnType();
    if (!type.isPrimitive()) {
      return null;
    }
    if (type == boolean.class) {
      return false;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == double.class) {
      return 0.0d;
    }
    if (type == float.class) {
      return 0.0f;
    }
    if (type == short.class) {
      return (short) 0;
    }
    if (type == byte.class) {
      return (byte) 0;
    }
    if (type == char.class) {
      return (char) 0;
    }
    return null;
  }
}
