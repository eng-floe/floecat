package ai.floedb.metacat.connector.delta.uc.impl;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class DeltaFileEnumerator {
  private final List<FileMeta> data = new ArrayList<>();
  private final List<DeleteMeta> deletes = new ArrayList<>();

  static final class FileMeta {
    private final String path;
    private final long sizeBytes;
    private final long rowCount;

    FileMeta(String path, long sizeBytes, long rowCount) {
      this.path = path;
      this.sizeBytes = sizeBytes;
      this.rowCount = rowCount;
    }

    String path() {
      return path;
    }

    long sizeBytes() {
      return sizeBytes;
    }

    long rowCount() {
      return rowCount;
    }
  }

  static final class DeleteMeta {
    private final String path;
    private final long sizeBytes;
    private final long rowCount;
    private final boolean equality;

    DeleteMeta(String path, long sizeBytes, long rowCount, boolean equality) {
      this.path = path;
      this.sizeBytes = sizeBytes;
      this.rowCount = rowCount;
      this.equality = equality;
    }

    String path() {
      return path;
    }

    long sizeBytes() {
      return sizeBytes;
    }

    long rowCount() {
      return rowCount;
    }

    boolean isEquality() {
      return equality;
    }
  }

  DeltaFileEnumerator(Engine engine, String tableRoot, long version) {
    Objects.requireNonNull(engine);
    Objects.requireNonNull(tableRoot);
    Table table = Table.forPath(engine, tableRoot);
    Snapshot snap = table.getSnapshotAsOfVersion(engine, version);

    ScanBuilder sb = snap.getScanBuilder();
    Scan scan = sb.build();

    try (CloseableIterator<FilteredColumnarBatch> it = scan.getScanFiles(engine)) {
      while (it.hasNext()) {
        try (var rows = it.next().getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();
            FileStatus fs = InternalScanFileUtils.getAddFileStatus(scanFileRow);
            data.add(new FileMeta(fs.getPath(), fs.getSize(), 0L));
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Enumerating Delta files failed (version " + version + ")", e);
    }
  }

  List<FileMeta> dataFiles() {
    return Collections.unmodifiableList(data);
  }

  List<DeleteMeta> deleteFiles() {
    return Collections.unmodifiableList(deletes);
  }
}
