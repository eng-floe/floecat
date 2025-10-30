package ai.floedb.metacat.connector.common.ndv;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public final class ParquetNdvProvider implements NdvProvider {

  private final Function<String, org.apache.iceberg.io.InputFile> icebergLookup;
  private final int thetaK;

  public ParquetNdvProvider(Function<String, org.apache.iceberg.io.InputFile> icebergLookup) {
    this(icebergLookup, 4096);
  }

  public ParquetNdvProvider(
      Function<String, org.apache.iceberg.io.InputFile> icebergLookup, int thetaK) {
    this.icebergLookup = Objects.requireNonNull(icebergLookup, "icebergLookup");

    if (thetaK < 16) {
      throw new IllegalArgumentException("thetaK too small");
    }

    this.thetaK = thetaK;
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) {
    if (sinks == null || sinks.isEmpty()) {
      return;
    }

    long totalRowsSeen = 0L;

    try (ParquetFileReader reader = open(filePath)) {
      final MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();

      final List<String> present =
          sinks.keySet().stream().filter(fileSchema::containsField).collect(Collectors.toList());

      final List<String> limited = present.size() > 32 ? present.subList(0, 32) : present;

      if (limited.isEmpty()) {
        return;
      }

      final Map<String, UpdateSketch> thetaByCol = new HashMap<>();
      for (String columnName : limited) {
        thetaByCol.put(columnName, UpdateSketch.builder().setNominalEntries(thetaK).build());
      }

      final List<Type> types = new ArrayList<>(limited.size());
      for (String columnName : limited) {
        types.add(fileSchema.getType(columnName));
      }
      final MessageType projection = new MessageType(fileSchema.getName(), types);

      final ColumnIOFactory cioFactory = new ColumnIOFactory();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        final long rowCount = pages.getRowCount();
        totalRowsSeen += rowCount;

        final MessageColumnIO columnIO = cioFactory.getColumnIO(projection, fileSchema);
        final GroupRecordConverter converter = new GroupRecordConverter(projection);
        final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, converter);
        for (long rowIndex = 0; rowIndex < rowCount; rowIndex++) {
          final Group g = recordReader.read();

          for (int columnIndex = 0; columnIndex < projection.getFieldCount(); columnIndex++) {
            final String colName = projection.getFieldName(columnIndex);

            if (g.getFieldRepetitionCount(columnIndex) == 0) {
              continue;
            }

            final Type type = projection.getType(columnIndex);
            try {
              if (!type.isPrimitive()) {
                updateSketch(thetaByCol, colName, g.getValueToString(columnIndex, 0));
              } else {
                final PrimitiveType.PrimitiveTypeName p =
                    type.asPrimitiveType().getPrimitiveTypeName();
                final LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
                switch (p) {
                  case INT32 -> {
                    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                      updateSketch(thetaByCol, colName, g.getValueToString(columnIndex, 0));
                    } else {
                      updateSketch(thetaByCol, colName, g.getInteger(columnIndex, 0));
                    }
                  }
                  case INT64 -> {
                    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                      updateSketch(thetaByCol, colName, g.getValueToString(columnIndex, 0));
                    } else {
                      updateSketch(thetaByCol, colName, g.getLong(columnIndex, 0));
                    }
                  }
                  case FLOAT -> updateSketch(thetaByCol, colName, g.getFloat(columnIndex, 0));
                  case DOUBLE -> updateSketch(thetaByCol, colName, g.getDouble(columnIndex, 0));
                  case BOOLEAN -> updateSketch(thetaByCol, colName, g.getBoolean(columnIndex, 0));
                  case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                    updateSketch(thetaByCol, colName, g.getValueToString(columnIndex, 0));
                  }
                  default -> updateSketch(thetaByCol, colName, g.getValueToString(columnIndex, 0));
                }
              }
            } catch (ClassCastException cce) {
              updateSketch(thetaByCol, colName, g.getValueToString(columnIndex, 0));
            }
          }
        }
      }

      for (String column : limited) {
        ColumnNdv out = sinks.get(column);
        if (out == null) {
          continue;
        }

        if (out.approx == null) {
          out.approx = new NdvApprox();
        }

        UpdateSketch updateSketch = thetaByCol.get(column);
        if (updateSketch == null) {
          continue;
        }

        CompactSketch compactSketch = updateSketch.compact(true, null);
        out.mergeTheta(compactSketch);

        out.approx.estimate = compactSketch.getEstimate();
        out.approx.method = "apache-datasketches-theta";
        out.approx.rowsSeen =
            (out.approx.rowsSeen == null ? 0L : out.approx.rowsSeen) + totalRowsSeen;
      }
    } catch (Exception e) {
      throw new RuntimeException("NDV scan failed for " + filePath, e);
    }
  }

  private static void updateSketch(Map<String, UpdateSketch> thetaByCol, String col, int value) {
    thetaByCol.get(col).update(value);
  }

  private static void updateSketch(Map<String, UpdateSketch> thetaByCol, String col, long value) {
    thetaByCol.get(col).update(value);
  }

  private static void updateSketch(
      Map<String, UpdateSketch> thetaByCol, String col, boolean value) {
    thetaByCol.get(col).update(value ? 1 : 0);
  }

  private static void updateSketch(Map<String, UpdateSketch> thetaByCol, String col, float value) {
    int bits = Float.floatToIntBits(value);
    thetaByCol.get(col).update(bits);
  }

  private static void updateSketch(Map<String, UpdateSketch> thetaByCol, String col, double value) {
    long bits = Double.doubleToLongBits(value);
    thetaByCol.get(col).update(bits);
  }

  private static void updateSketch(Map<String, UpdateSketch> thetaByCol, String col, String value) {
    if (value == null) {
      value = "";
    }
    thetaByCol.get(col).update(value);
  }

  private ParquetFileReader open(String path) throws IOException {
    org.apache.iceberg.io.InputFile ifile = icebergLookup.apply(path);
    return ParquetFileReader.open(new ParquetInputFileAdapter(ifile));
  }

  private static final class ParquetInputFileAdapter implements InputFile {
    private final org.apache.iceberg.io.InputFile inputFile;

    ParquetInputFileAdapter(org.apache.iceberg.io.InputFile in) {
      this.inputFile = in;
    }

    @Override
    public long getLength() throws IOException {
      return inputFile.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return new SeekableAdapter(inputFile.newStream());
    }
  }

  private static final class SeekableAdapter extends SeekableInputStream {
    private static final int TMP_BUF_SIZE = 8192;
    private final org.apache.iceberg.io.SeekableInputStream d;

    SeekableAdapter(org.apache.iceberg.io.SeekableInputStream d) {
      this.d = d;
    }

    @Override
    public long getPos() throws IOException {
      return d.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      d.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return d.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return d.read(b, off, len);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (!dst.hasRemaining()) {
        return 0;
      }

      if (dst.hasArray()) {
        int pos = dst.position();
        int n = d.read(dst.array(), dst.arrayOffset() + pos, dst.remaining());
        if (n > 0) {
          dst.position(pos + n);
        }

        return n;
      } else {
        byte[] tmp = new byte[Math.min(dst.remaining(), TMP_BUF_SIZE)];
        int total = 0;
        while (dst.hasRemaining()) {
          int toRead = Math.min(tmp.length, dst.remaining());
          int n = d.read(tmp, 0, toRead);
          if (n <= 0) break;
          dst.put(tmp, 0, n);
          total += n;
          if (n < toRead) break;
        }

        return total == 0 ? -1 : total;
      }
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      int off = 0, len = bytes.length;
      while (len > 0) {
        int n = d.read(bytes, off, len);
        if (n < 0) {
          throw new EOFException("EOF while reading fully");
        }
        off += n;
        len -= n;
      }
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
      int o = off, remaining = len;
      while (remaining > 0) {
        int n = d.read(bytes, o, remaining);
        if (n < 0) {
          throw new EOFException("EOF while reading fully");
        }
        o += n;
        remaining -= n;
      }
    }

    @Override
    public void readFully(ByteBuffer dst) throws IOException {
      if (!dst.hasRemaining()) {
        return;
      }

      if (dst.hasArray()) {
        readFully(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
        dst.position(dst.limit());
      } else {
        byte[] tmp = new byte[Math.min(dst.remaining(), TMP_BUF_SIZE)];
        while (dst.hasRemaining()) {
          int toRead = Math.min(tmp.length, dst.remaining());
          readFully(tmp, 0, toRead);
          dst.put(tmp, 0, toRead);
        }
      }
    }

    @Override
    public void close() throws IOException {
      d.close();
    }
  }
}
