/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.connector.iceberg.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

class IcebergConnectorSnapshotDeltaTest {
  @Test
  void manifestRemovalStopsBeforeFileChangesWithoutUsingNumericSnapshotOrder() {
    AtomicBoolean additionsRead = new AtomicBoolean();
    AtomicBoolean removalsRead = new AtomicBoolean();
    Snapshot base = snapshot(50L, null, List.of(), new AtomicBoolean(), new AtomicBoolean());
    Snapshot target = snapshot(10L, 50L, List.of(manifest(10L, 1, 1)), additionsRead, removalsRead);
    Table table = table(base, target);
    IcebergConnector connector = connector(table);

    var delta =
        connector.planSnapshotFileDelta("ns", "table", ResourceId.getDefaultInstance(), 50L, 10L);

    assertTrue(delta.isEmpty());
    assertFalse(additionsRead.get());
    assertFalse(removalsRead.get());
  }

  private static IcebergConnector connector(Table table) {
    return new IcebergConnector("test", null, null, null, false, 0.0d, 0L, null) {
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
  }

  private static Snapshot snapshot(
      long snapshotId,
      Long parentSnapshotId,
      List<ManifestFile> dataManifests,
      AtomicBoolean additionsRead,
      AtomicBoolean removalsRead) {
    return (Snapshot)
        Proxy.newProxyInstance(
            Snapshot.class.getClassLoader(),
            new Class<?>[] {Snapshot.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "snapshotId" -> snapshotId;
                  case "parentId" -> parentSnapshotId;
                  case "schemaId" -> null;
                  case "dataManifests" -> dataManifests;
                  case "deleteManifests" -> List.of();
                  case "removedDataFiles" -> {
                    removalsRead.set(true);
                    yield List.of();
                  }
                  case "addedDataFiles" -> {
                    additionsRead.set(true);
                    yield List.of();
                  }
                  default -> defaultValue(method);
                });
  }

  private static ManifestFile manifest(long snapshotId, int addedFiles, int deletedFiles) {
    return (ManifestFile)
        Proxy.newProxyInstance(
            ManifestFile.class.getClassLoader(),
            new Class<?>[] {ManifestFile.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "snapshotId" -> snapshotId;
                  case "addedFilesCount" -> addedFiles;
                  case "deletedFilesCount" -> deletedFiles;
                  default -> defaultValue(method);
                });
  }

  private static Table table(Snapshot base, Snapshot target) {
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "snapshot" -> ((Long) args[0]) == 50L ? base : target;
                  case "schema" -> new Schema();
                  case "schemas", "properties", "sortOrders", "refs" -> Map.of();
                  default -> defaultValue(method);
                });
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
