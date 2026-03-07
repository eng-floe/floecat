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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            "snapshotsToEnumerate", Table.class, boolean.class, Set.class, Set.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Snapshot> snapshots =
        (List<Snapshot>) method.invoke(connector, table, false, Set.of(100L, 200L), Set.of());

    assertEquals(List.of(300L, 250L), snapshots.stream().map(Snapshot::snapshotId).toList());
  }

  @Test
  void snapshotsToEnumerateFullRescanStillHonorsExplicitSnapshotScope() throws Exception {
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
    Table table = table(List.of(latest, target, earlier), latest);

    Method method =
        IcebergConnector.class.getDeclaredMethod(
            "snapshotsToEnumerate", Table.class, boolean.class, Set.class, Set.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Snapshot> snapshots =
        (List<Snapshot>) method.invoke(connector, table, true, Set.of(), Set.of(200L));

    assertEquals(List.of(200L), snapshots.stream().map(Snapshot::snapshotId).toList());
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
    InvocationHandler handler =
        (proxy, method, args) -> {
          return switch (method.getName()) {
            case "snapshots" -> snapshots;
            case "currentSnapshot" -> current;
            case "snapshot" -> {
              Long id = (Long) args[0];
              yield snapshots.stream().filter(s -> s.snapshotId() == id).findFirst().orElse(null);
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
