package ai.floedb.metacat.connector.common.ndv;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  public ParquetNdvProvider(Function<String, org.apache.iceberg.io.InputFile> icebergLookup) {
    this.icebergLookup = Objects.requireNonNull(icebergLookup, "icebergLookup");
  }

  @Override
  public void contributeNdv(String filePath, Map<String, Hll> sinks) {
    if (sinks == null || sinks.isEmpty()) {
      return;
    }

    try (ParquetFileReader reader = open(filePath)) {
      final MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();

      final List<String> present =
          sinks.keySet().stream().filter(fileSchema::containsField).collect(Collectors.toList());
      if (present.isEmpty()) {
        return;
      }

      final List<Type> types = new ArrayList<>(present.size());
      for (String c : present) types.add(fileSchema.getType(c));
      final MessageType proj = new MessageType(fileSchema.getName(), types);

      final ColumnIOFactory cioFactory = new ColumnIOFactory();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        final long rowCount = pages.getRowCount();
        final MessageColumnIO columnIO = cioFactory.getColumnIO(proj);
        final GroupRecordConverter conv = new GroupRecordConverter(proj);
        final RecordReader<Group> rr = columnIO.getRecordReader(pages, conv);

        for (long r = 0; r < rowCount; r++) {
          final Group g = rr.read();

          for (int i = 0; i < proj.getFieldCount(); i++) {
            final String col = proj.getFieldName(i);
            final Hll h = sinks.get(col);

            if (h == null) {
              continue;
            }

            if (g.getFieldRepetitionCount(i) == 0) {
              continue;
            }

            final Type t = proj.getType(i);
            if (!t.isPrimitive()) {
              h.addString(g.getValueToString(i, 0));
              continue;
            }

            final PrimitiveType.PrimitiveTypeName p = t.asPrimitiveType().getPrimitiveTypeName();
            final LogicalTypeAnnotation lt = t.getLogicalTypeAnnotation();

            try {
              switch (p) {
                case INT32 -> {
                  if (lt instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    h.addString(g.getValueToString(i, 0));
                  } else {
                    h.addLong(NdvHash.hash64(g.getInteger(i, 0)));
                  }
                }
                case INT64 -> {
                  if (lt instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    h.addString(g.getValueToString(i, 0));
                  } else {
                    h.addLong(NdvHash.hash64(g.getLong(i, 0)));
                  }
                }
                case FLOAT -> {
                  int bits = Float.floatToIntBits(g.getFloat(i, 0));
                  h.addLong(NdvHash.hash64(bits));
                }
                case DOUBLE -> {
                  long bits = Double.doubleToLongBits(g.getDouble(i, 0));
                  h.addLong(NdvHash.hash64(bits));
                }
                case BOOLEAN -> {
                  h.addLong(g.getBoolean(i, 0) ? 1L : 0L);
                }
                case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                  h.addString(g.getValueToString(i, 0));
                }
                case INT96 -> {
                  h.addString(g.getValueToString(i, 0));
                }
                default -> h.addString(g.getValueToString(i, 0));
              }
            } catch (ClassCastException cce) {
              h.addString(g.getValueToString(i, 0));
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("NDV scan failed for " + filePath, e);
    }
  }

  private ParquetFileReader open(String path) throws IOException {
    org.apache.iceberg.io.InputFile ifile = icebergLookup.apply(path);
    return ParquetFileReader.open(new ParquetInputFileAdapter(ifile));
  }

  private static final class ParquetInputFileAdapter implements InputFile {
    private final org.apache.iceberg.io.InputFile in;

    ParquetInputFileAdapter(org.apache.iceberg.io.InputFile in) {
      this.in = in;
    }

    @Override
    public long getLength() throws IOException {
      return in.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return new SeekableAdapter(in.newStream());
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
        if (n > 0) dst.position(pos + n);
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
      int off = 0;
      int len = bytes.length;
      while (len > 0) {
        int n = d.read(bytes, off, len);
        if (n < 0) throw new EOFException("EOF while reading fully");
        off += n;
        len -= n;
      }
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
      int o = off, remaining = len;
      while (remaining > 0) {
        int n = d.read(bytes, o, remaining);
        if (n < 0) throw new EOFException("EOF while reading fully");
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
