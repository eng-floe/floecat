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

package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

public final class ParquetPageIndexReader {
  private static final EnumSet<PrimitiveType.PrimitiveTypeName> SUPPORTED_PRIMITIVE_TYPES =
      EnumSet.of(
          PrimitiveType.PrimitiveTypeName.BOOLEAN,
          PrimitiveType.PrimitiveTypeName.INT32,
          PrimitiveType.PrimitiveTypeName.INT64,
          PrimitiveType.PrimitiveTypeName.FLOAT,
          PrimitiveType.PrimitiveTypeName.DOUBLE);
  private final Function<String, org.apache.parquet.io.InputFile> parquetLookup;

  public ParquetPageIndexReader(Function<String, org.apache.parquet.io.InputFile> parquetLookup) {
    this.parquetLookup = Objects.requireNonNull(parquetLookup, "parquetLookup");
  }

  public static ParquetPageIndexReader forIcebergIO(
      Function<String, org.apache.iceberg.io.InputFile> icebergLookup) {
    return new ParquetPageIndexReader(
        path -> new IcebergParquetInputFileAdapter(icebergLookup.apply(path)));
  }

  public List<FloecatConnector.ParquetPageIndexEntry> readEntries(Set<String> plannedFilePaths) {
    if (plannedFilePaths == null || plannedFilePaths.isEmpty()) {
      return List.of();
    }
    List<FloecatConnector.ParquetPageIndexEntry> out = new ArrayList<>();
    for (String filePath : plannedFilePaths) {
      if (!isParquetPath(filePath)) {
        continue;
      }
      out.addAll(readEntries(filePath));
    }
    return List.copyOf(out);
  }

  public List<FloecatConnector.ParquetPageIndexEntry> readEntries(String filePath) {
    if (filePath == null || filePath.isBlank() || !isParquetPath(filePath)) {
      return List.of();
    }
    InputFile inputFile = parquetLookup.apply(filePath);
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      var footer = reader.getFooter();
      var schema = footer.getFileMetaData().getSchema();
      var parsedVersion = parsedVersion(footer.getFileMetaData().getCreatedBy());
      List<FloecatConnector.ParquetPageIndexEntry> out = new ArrayList<>();
      var blocks = footer.getBlocks();
      for (int rowGroupOrdinal = 0; rowGroupOrdinal < blocks.size(); rowGroupOrdinal++) {
        var block = blocks.get(rowGroupOrdinal);
        try (PageReadStore pageStore = reader.readRowGroup(rowGroupOrdinal)) {
          for (var column : block.getColumns()) {
            String[] path = column.getPath().toArray();
            ColumnDescriptor descriptor = schema.getColumnDescription(path);
            if (!supportsPageIndexDecoding(descriptor, column.getPrimitiveType())) {
              continue;
            }
            String columnName = String.join(".", path);
            var decimal =
                decimalMetadata(column.getPrimitiveType().getLogicalTypeAnnotation(), column);
            long blockRowCount = block.getRowCount();
            PageReader pageReader = pageStore.getPageReader(descriptor);
            out.addAll(
                readChunkPages(
                    inputFile,
                    filePath,
                    columnName,
                    rowGroupOrdinal,
                    blockRowCount,
                    column,
                    descriptor,
                    decimal,
                    pageReader,
                    parsedVersion));
          }
        }
      }
      return List.copyOf(out);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read parquet page indexes for " + filePath, e);
    }
  }

  private static List<FloecatConnector.ParquetPageIndexEntry> readChunkPages(
      InputFile inputFile,
      String filePath,
      String columnName,
      int rowGroupOrdinal,
      long blockRowCount,
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column,
      ColumnDescriptor descriptor,
      DecimalMetadata decimal,
      PageReader pageReader,
      VersionParser.ParsedVersion parsedVersion)
      throws IOException {
    byte[] chunkBytes = readChunkBytes(inputFile, column);
    DictionaryPage dictionaryPage = pageReader == null ? null : pageReader.readDictionaryPage();
    List<PageEnvelope> pageEnvelopes = new ArrayList<>();
    List<FloecatConnector.ParquetPageIndexEntry> out = new ArrayList<>();
    Long dictionaryPageHeaderOffset = null;
    Integer dictionaryPageTotalCompressedSize = null;

    try (CountingInputStream in = new CountingInputStream(new ByteArrayInputStream(chunkBytes))) {
      while (in.getCount() < chunkBytes.length) {
        long chunkOffset = in.getCount();
        long absoluteOffset = column.getStartingPos() + chunkOffset;
        PageHeader pageHeader = Util.readPageHeader(in);
        int headerSize = (int) (in.getCount() - chunkOffset);
        int totalCompressedSize = safeAdd(headerSize, pageHeader.getCompressed_page_size());

        if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
          if (dictionaryPageHeaderOffset != null) {
            throw new IOException(
                "Multiple dictionary pages found for " + filePath + " column " + column.getPath());
          }
          dictionaryPageHeaderOffset = absoluteOffset;
          dictionaryPageTotalCompressedSize = totalCompressedSize;
          skipFully(in, pageHeader.getCompressed_page_size());
          continue;
        }

        if (pageHeader.getType() == PageType.DATA_PAGE
            || pageHeader.getType() == PageType.DATA_PAGE_V2) {
          pageEnvelopes.add(
              new PageEnvelope(
                  absoluteOffset,
                  totalCompressedSize,
                  requiresDictionary(pageHeader),
                  dictionaryPageHeaderOffset,
                  dictionaryPageTotalCompressedSize));
        } else if (pageHeader.getType() != PageType.INDEX_PAGE) {
          throw new IOException(
              "Unsupported parquet page type "
                  + pageHeader.getType()
                  + " for "
                  + filePath
                  + " column "
                  + columnName);
        }

        skipFully(in, pageHeader.getCompressed_page_size());
      }
    }

    long firstRowIndex = 0L;
    int pageOrdinal = 0;
    while (pageReader != null) {
      DataPage dataPage = pageReader.readPage();
      if (dataPage == null) {
        break;
      }
      if (pageOrdinal >= pageEnvelopes.size()) {
        throw new IOException(
            "Decoded more parquet data pages than raw page headers for "
                + filePath
                + " column "
                + columnName);
      }
      int rowCount = pageRowCount(dataPage, blockRowCount, firstRowIndex);
      TypedStats typedStats =
          decodeTypedStats(
              descriptor,
              column.getPrimitiveType(),
              column.getPrimitiveType().getLogicalTypeAnnotation(),
              dictionaryPage,
              dataPage,
              rowCount,
              parsedVersion);
      PageEnvelope envelope = pageEnvelopes.get(pageOrdinal);
      out.add(
          new FloecatConnector.ParquetPageIndexEntry(
              filePath,
              columnName,
              rowGroupOrdinal,
              pageOrdinal,
              firstRowIndex,
              rowCount,
              rowCount,
              envelope.pageHeaderOffset(),
              envelope.pageTotalCompressedSize(),
              envelope.dictionaryPageHeaderOffset(),
              envelope.dictionaryPageTotalCompressedSize(),
              envelope.requiresDictionaryPage(),
              column.getPrimitiveType().getPrimitiveTypeName().name(),
              parquetCompression(column),
              (short) descriptor.getMaxDefinitionLevel(),
              (short) descriptor.getMaxRepetitionLevel(),
              decimal.precision(),
              decimal.scale(),
              decimal.bits(),
              typedStats.minI32(),
              typedStats.maxI32(),
              typedStats.minI64(),
              typedStats.maxI64(),
              typedStats.minF32(),
              typedStats.maxF32(),
              typedStats.minF64(),
              typedStats.maxF64(),
              typedStats.minBool(),
              typedStats.maxBool(),
              typedStats.minUtf8(),
              typedStats.maxUtf8(),
              typedStats.minDecimal128Unscaled(),
              typedStats.maxDecimal128Unscaled(),
              typedStats.minDecimal256Unscaled(),
              typedStats.maxDecimal256Unscaled()));
      firstRowIndex += rowCount;
      pageOrdinal += 1;
    }

    if (pageOrdinal != pageEnvelopes.size()) {
      throw new IOException(
          "Decoded fewer parquet data pages than raw page headers for "
              + filePath
              + " column "
              + columnName);
    }
    return List.copyOf(out);
  }

  private static byte[] readChunkBytes(
      InputFile inputFile, org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column)
      throws IOException {
    byte[] chunkBytes = new byte[clampInt(column.getTotalSize())];
    try (SeekableInputStream in = inputFile.newStream()) {
      in.seek(column.getStartingPos());
      in.readFully(chunkBytes);
    }
    return chunkBytes;
  }

  private static int pageRowCount(DataPage page, long blockRowCount, long rowsSeen) {
    if (page instanceof org.apache.parquet.column.page.DataPageV2 pageV2) {
      return Math.max(0, pageV2.getRowCount());
    }
    long remaining = Math.max(0L, blockRowCount - rowsSeen);
    return page.getValueCount() > 0 ? page.getValueCount() : clampInt(remaining);
  }

  private static boolean requiresDictionary(PageHeader pageHeader) {
    return switch (pageHeader.getType()) {
      case DATA_PAGE ->
          pageHeader.getData_page_header() != null
              && encodingUsesDictionary(pageHeader.getData_page_header().getEncoding());
      case DATA_PAGE_V2 ->
          pageHeader.getData_page_header_v2() != null
              && encodingUsesDictionary(pageHeader.getData_page_header_v2().getEncoding());
      default -> false;
    };
  }

  private static boolean encodingUsesDictionary(org.apache.parquet.format.Encoding encoding) {
    if (encoding == null) {
      return false;
    }
    return switch (encoding) {
      case PLAIN_DICTIONARY, RLE_DICTIONARY -> true;
      default -> false;
    };
  }

  private static void skipFully(InputStream in, int bytes) throws IOException {
    int remaining = Math.max(0, bytes);
    while (remaining > 0) {
      long skipped = in.skip(remaining);
      if (skipped <= 0) {
        if (in.read() < 0) {
          throw new IOException("Unexpected EOF while skipping parquet page payload");
        }
        remaining -= 1;
      } else {
        remaining -= (int) skipped;
      }
    }
  }

  private static int safeAdd(int left, int right) {
    long sum = (long) left + right;
    return sum > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0L, sum);
  }

  private static boolean isParquetPath(String filePath) {
    if (filePath == null || filePath.isBlank()) {
      return false;
    }
    String normalized = filePath.toLowerCase();
    return normalized.endsWith(".parquet") || normalized.endsWith(".parq");
  }

  private static Long nullIfNegative(long value) {
    return value < 0L ? null : value;
  }

  private static Integer dictionaryPageCompressedSize(
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column,
      Long dictionaryPageHeaderOffset) {
    if (dictionaryPageHeaderOffset == null) {
      return null;
    }
    long firstDataPageOffset = column.getFirstDataPageOffset();
    if (firstDataPageOffset <= dictionaryPageHeaderOffset) {
      return null;
    }
    return clampInt(firstDataPageOffset - dictionaryPageHeaderOffset);
  }

  private static int clampInt(long value) {
    if (value <= 0L) {
      return 0;
    }
    return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }

  private static final class CountingInputStream extends FilterInputStream {
    private long count = 0L;

    private CountingInputStream(InputStream in) {
      super(in);
    }

    private long getCount() {
      return count;
    }

    @Override
    public int read() throws IOException {
      int value = super.read();
      if (value >= 0) {
        count += 1;
      }
      return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = super.read(b, off, len);
      if (read > 0) {
        count += read;
      }
      return read;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = super.skip(n);
      if (skipped > 0) {
        count += skipped;
      }
      return skipped;
    }
  }

  private static String parquetCompression(
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column) {
    return switch (column.getCodec()) {
      case ZSTD -> "ZSTD(ZstdLevel(1))";
      default -> column.getCodec().name();
    };
  }

  private static DecimalMetadata decimalMetadata(
      LogicalTypeAnnotation logicalTypeAnnotation,
      org.apache.parquet.hadoop.metadata.ColumnChunkMetaData column) {
    if (!(logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation d)) {
      return DecimalMetadata.empty();
    }
    int bits =
        switch (column.getPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> 32;
          case INT64 -> 64;
          case FIXED_LEN_BYTE_ARRAY -> column.getPrimitiveType().getTypeLength() * 8;
          default -> 0;
        };
    return new DecimalMetadata(d.getPrecision(), d.getScale(), bits == 0 ? null : bits);
  }

  private record DecimalMetadata(Integer precision, Integer scale, Integer bits) {
    private static DecimalMetadata empty() {
      return new DecimalMetadata(null, null, null);
    }
  }

  private record PageEnvelope(
      long pageHeaderOffset,
      int pageTotalCompressedSize,
      boolean requiresDictionaryPage,
      Long dictionaryPageHeaderOffset,
      Integer dictionaryPageTotalCompressedSize) {}

  private static boolean supportsPageIndexDecoding(
      ColumnDescriptor descriptor, PrimitiveType primitiveType) {
    if (descriptor == null || primitiveType == null || descriptor.getMaxRepetitionLevel() != 0) {
      return false;
    }
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
    if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY;
    }
    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
          || primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
          || primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
          || primitiveTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
    }
    return SUPPORTED_PRIMITIVE_TYPES.contains(primitiveTypeName);
  }

  private static TypedStats decodeTypedStats(
      ColumnDescriptor descriptor,
      PrimitiveType primitiveType,
      LogicalTypeAnnotation logicalType,
      DictionaryPage dictionaryPage,
      DataPage dataPage,
      int expectedRows,
      VersionParser.ParsedVersion parsedVersion)
      throws IOException {
    var pageReader = new SinglePageReader(dictionaryPage, dataPage);
    ColumnReader columnReader =
        new ColumnReaderImpl(descriptor, pageReader, new PrimitiveConverter() {}, parsedVersion);
    MutableTypedStats stats = new MutableTypedStats(logicalType);
    int maxDefinitionLevel = descriptor.getMaxDefinitionLevel();
    for (int i = 0; i < expectedRows; i++) {
      if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
        stats.observe(columnReader, primitiveType, logicalType);
      }
      columnReader.consume();
    }
    return stats.toTypedStats();
  }

  private static VersionParser.ParsedVersion parsedVersion(String createdBy) {
    if (createdBy == null || createdBy.isBlank()) {
      return null;
    }
    try {
      return VersionParser.parse(createdBy);
    } catch (Exception ignored) {
      return null;
    }
  }

  private static final class SinglePageReader implements PageReader {
    private final DictionaryPage dictionaryPage;
    private final DataPage dataPage;
    private boolean dictionaryRead = false;
    private boolean pageRead = false;

    private SinglePageReader(DictionaryPage dictionaryPage, DataPage dataPage) {
      this.dictionaryPage = dictionaryPage;
      this.dataPage = dataPage;
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (dictionaryRead || dictionaryPage == null) {
        return null;
      }
      dictionaryRead = true;
      return dictionaryPage;
    }

    @Override
    public long getTotalValueCount() {
      return dataPage.getValueCount();
    }

    @Override
    public DataPage readPage() {
      if (pageRead) {
        return null;
      }
      pageRead = true;
      return dataPage;
    }
  }

  private static final class MutableTypedStats {
    private Integer minI32;
    private Integer maxI32;
    private Long minI64;
    private Long maxI64;
    private Float minF32;
    private Float maxF32;
    private Double minF64;
    private Double maxF64;
    private Boolean minBool;
    private Boolean maxBool;
    private String minUtf8;
    private String maxUtf8;
    private BigInteger minDecimal128;
    private BigInteger maxDecimal128;
    private BigInteger minDecimal256;
    private BigInteger maxDecimal256;

    private MutableTypedStats(LogicalTypeAnnotation logicalType) {}

    private void observe(
        ColumnReader columnReader,
        PrimitiveType primitiveType,
        LogicalTypeAnnotation logicalTypeAnnotation) {
      if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
        updateUtf8(columnReader.getBinary().toStringUsingUTF8());
        return;
      }
      if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation d) {
        observeDecimal(columnReader, primitiveType, d.getPrecision());
        return;
      }
      switch (primitiveType.getPrimitiveTypeName()) {
        case INT32 -> updateInt32(columnReader.getInteger());
        case INT64 -> updateInt64(columnReader.getLong());
        case FLOAT -> updateFloat32(columnReader.getFloat());
        case DOUBLE -> updateFloat64(columnReader.getDouble());
        case BOOLEAN -> updateBoolean(columnReader.getBoolean());
        default -> {
          // No typed sidecar stats for unsupported physical types in the OSS Java path.
        }
      }
    }

    private void observeDecimal(
        ColumnReader columnReader, PrimitiveType primitiveType, int precision) {
      int width = precision <= 38 ? 16 : 32;
      BigInteger value =
          switch (primitiveType.getPrimitiveTypeName()) {
            case INT32 -> BigInteger.valueOf(columnReader.getInteger());
            case INT64 -> BigInteger.valueOf(columnReader.getLong());
            case FIXED_LEN_BYTE_ARRAY, BINARY ->
                new BigInteger(columnReader.getBinary().getBytes());
            default -> null;
          };
      if (value == null) {
        return;
      }
      if (width == 16) {
        minDecimal128 =
            minDecimal128 == null || value.compareTo(minDecimal128) < 0 ? value : minDecimal128;
        maxDecimal128 =
            maxDecimal128 == null || value.compareTo(maxDecimal128) > 0 ? value : maxDecimal128;
      } else {
        minDecimal256 =
            minDecimal256 == null || value.compareTo(minDecimal256) < 0 ? value : minDecimal256;
        maxDecimal256 =
            maxDecimal256 == null || value.compareTo(maxDecimal256) > 0 ? value : maxDecimal256;
      }
    }

    private void updateInt32(int value) {
      minI32 = minI32 == null ? value : Math.min(minI32, value);
      maxI32 = maxI32 == null ? value : Math.max(maxI32, value);
    }

    private void updateInt64(long value) {
      minI64 = minI64 == null ? value : Math.min(minI64, value);
      maxI64 = maxI64 == null ? value : Math.max(maxI64, value);
    }

    private void updateFloat32(float value) {
      minF32 = minF32 == null ? value : Math.min(minF32, value);
      maxF32 = maxF32 == null ? value : Math.max(maxF32, value);
    }

    private void updateFloat64(double value) {
      minF64 = minF64 == null ? value : Math.min(minF64, value);
      maxF64 = maxF64 == null ? value : Math.max(maxF64, value);
    }

    private void updateBoolean(boolean value) {
      minBool = minBool == null ? value : (minBool && value);
      maxBool = maxBool == null ? value : (maxBool || value);
    }

    private void updateUtf8(String value) {
      minUtf8 = minUtf8 == null || value.compareTo(minUtf8) < 0 ? value : minUtf8;
      maxUtf8 = maxUtf8 == null || value.compareTo(maxUtf8) > 0 ? value : maxUtf8;
    }

    private TypedStats toTypedStats() {
      return new TypedStats(
          minI32,
          maxI32,
          minI64,
          maxI64,
          minF32,
          maxF32,
          minF64,
          maxF64,
          minBool,
          maxBool,
          minUtf8,
          maxUtf8,
          minDecimal128 == null ? null : toFixedLenBytes(minDecimal128, 16),
          maxDecimal128 == null ? null : toFixedLenBytes(maxDecimal128, 16),
          minDecimal256 == null ? null : toFixedLenBytes(minDecimal256, 32),
          maxDecimal256 == null ? null : toFixedLenBytes(maxDecimal256, 32));
    }
  }

  private static TypedStats typedStats(
      PrimitiveType primitiveType,
      LogicalTypeAnnotation logicalType,
      ColumnIndex columnIndex,
      int pageIndex) {
    if (columnIndex == null
        || pageIndex < 0
        || pageIndex >= columnIndex.getMinValues().size()
        || pageIndex >= columnIndex.getMaxValues().size()) {
      return TypedStats.empty();
    }
    ByteBuffer min = duplicate(columnIndex.getMinValues().get(pageIndex));
    ByteBuffer max = duplicate(columnIndex.getMaxValues().get(pageIndex));
    if (min == null || max == null) {
      return TypedStats.empty();
    }
    if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return new TypedStats(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          decodeUtf8(min),
          decodeUtf8(max),
          null,
          null,
          null,
          null);
    }
    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return decimalTypedStats(primitiveType, min, max);
    }
    return switch (primitiveType.getPrimitiveTypeName()) {
      case INT32 ->
          new TypedStats(
              readInt32(min),
              readInt32(max),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case INT64 ->
          new TypedStats(
              null,
              null,
              readInt64(min),
              readInt64(max),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case FLOAT ->
          new TypedStats(
              null,
              null,
              null,
              null,
              readFloat32(min),
              readFloat32(max),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case DOUBLE ->
          new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              readFloat64(min),
              readFloat64(max),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case BOOLEAN ->
          new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              readBoolean(min),
              readBoolean(max),
              null,
              null,
              null,
              null,
              null,
              null);
      default -> TypedStats.empty();
    };
  }

  private static TypedStats typedStatsFromChunkStatistics(
      PrimitiveType primitiveType, LogicalTypeAnnotation logicalType, Statistics<?> statistics) {
    if (statistics == null || statistics.isEmpty()) {
      return TypedStats.empty();
    }
    Object min = statistics.genericGetMin();
    Object max = statistics.genericGetMax();
    if (min == null || max == null) {
      return TypedStats.empty();
    }
    if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return new TypedStats(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          min.toString(),
          max.toString(),
          null,
          null,
          null,
          null);
    }
    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return switch (primitiveType.getPrimitiveTypeName()) {
        case INT32 ->
            new TypedStats(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                toFixedLenBytes(((Number) min).intValue(), 16),
                toFixedLenBytes(((Number) max).intValue(), 16),
                null,
                null);
        case INT64 ->
            new TypedStats(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                toFixedLenBytes(((Number) min).longValue(), 16),
                toFixedLenBytes(((Number) max).longValue(), 16),
                null,
                null);
        default -> TypedStats.empty();
      };
    }
    return switch (primitiveType.getPrimitiveTypeName()) {
      case INT32 ->
          new TypedStats(
              ((Number) min).intValue(),
              ((Number) max).intValue(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case INT64 ->
          new TypedStats(
              null,
              null,
              ((Number) min).longValue(),
              ((Number) max).longValue(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case FLOAT ->
          new TypedStats(
              null,
              null,
              null,
              null,
              ((Number) min).floatValue(),
              ((Number) max).floatValue(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case DOUBLE ->
          new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              ((Number) min).doubleValue(),
              ((Number) max).doubleValue(),
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      case BOOLEAN ->
          new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              (Boolean) min,
              (Boolean) max,
              null,
              null,
              null,
              null,
              null,
              null);
      default -> TypedStats.empty();
    };
  }

  private static TypedStats mergeTypedStats(TypedStats primary, TypedStats fallback) {
    return new TypedStats(
        primary.minI32() != null ? primary.minI32() : fallback.minI32(),
        primary.maxI32() != null ? primary.maxI32() : fallback.maxI32(),
        primary.minI64() != null ? primary.minI64() : fallback.minI64(),
        primary.maxI64() != null ? primary.maxI64() : fallback.maxI64(),
        primary.minF32() != null ? primary.minF32() : fallback.minF32(),
        primary.maxF32() != null ? primary.maxF32() : fallback.maxF32(),
        primary.minF64() != null ? primary.minF64() : fallback.minF64(),
        primary.maxF64() != null ? primary.maxF64() : fallback.maxF64(),
        primary.minBool() != null ? primary.minBool() : fallback.minBool(),
        primary.maxBool() != null ? primary.maxBool() : fallback.maxBool(),
        primary.minUtf8() != null ? primary.minUtf8() : fallback.minUtf8(),
        primary.maxUtf8() != null ? primary.maxUtf8() : fallback.maxUtf8(),
        primary.minDecimal128Unscaled() != null
            ? primary.minDecimal128Unscaled()
            : fallback.minDecimal128Unscaled(),
        primary.maxDecimal128Unscaled() != null
            ? primary.maxDecimal128Unscaled()
            : fallback.maxDecimal128Unscaled(),
        primary.minDecimal256Unscaled() != null
            ? primary.minDecimal256Unscaled()
            : fallback.minDecimal256Unscaled(),
        primary.maxDecimal256Unscaled() != null
            ? primary.maxDecimal256Unscaled()
            : fallback.maxDecimal256Unscaled());
  }

  private static TypedStats decimalTypedStats(
      PrimitiveType primitiveType, ByteBuffer min, ByteBuffer max) {
    return switch (primitiveType.getPrimitiveTypeName()) {
      case INT32 ->
          new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              toFixedLenBytes(readInt32(min), 16),
              toFixedLenBytes(readInt32(max), 16),
              null,
              null);
      case INT64 ->
          new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              toFixedLenBytes(readInt64(min), 16),
              toFixedLenBytes(readInt64(max), 16),
              null,
              null);
      case FIXED_LEN_BYTE_ARRAY, BINARY -> {
        byte[] minBytes = bytes(min);
        byte[] maxBytes = bytes(max);
        if (minBytes.length <= 16 && maxBytes.length <= 16) {
          yield new TypedStats(
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              signExtend(minBytes, 16),
              signExtend(maxBytes, 16),
              null,
              null);
        }
        yield new TypedStats(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            signExtend(minBytes, 32),
            signExtend(maxBytes, 32));
      }
      default -> TypedStats.empty();
    };
  }

  private static ByteBuffer duplicate(ByteBuffer buffer) {
    return buffer == null ? null : buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
  }

  private static Integer readInt32(ByteBuffer buffer) {
    return buffer.remaining() < Integer.BYTES ? null : buffer.getInt(0);
  }

  private static Long readInt64(ByteBuffer buffer) {
    return buffer.remaining() < Long.BYTES ? null : buffer.getLong(0);
  }

  private static Float readFloat32(ByteBuffer buffer) {
    return buffer.remaining() < Float.BYTES ? null : buffer.getFloat(0);
  }

  private static Double readFloat64(ByteBuffer buffer) {
    return buffer.remaining() < Double.BYTES ? null : buffer.getDouble(0);
  }

  private static Boolean readBoolean(ByteBuffer buffer) {
    return buffer.remaining() < 1 ? null : buffer.get(0) != 0;
  }

  private static String decodeUtf8(ByteBuffer buffer) {
    return StandardCharsets.UTF_8
        .decode(buffer.duplicate().clear().position(buffer.position()).limit(buffer.limit()))
        .toString();
  }

  private static byte[] bytes(ByteBuffer buffer) {
    ByteBuffer duplicate = buffer.duplicate();
    byte[] out = new byte[duplicate.remaining()];
    duplicate.get(out);
    return out;
  }

  private static byte[] signExtend(byte[] input, int targetLength) {
    if (input.length > targetLength) {
      throw new IllegalArgumentException(
          "Value with " + input.length + " bytes does not fit in " + targetLength + " bytes");
    }
    byte sign = input.length > 0 && (input[0] & 0x80) != 0 ? (byte) 0xFF : 0x00;
    byte[] out = new byte[targetLength];
    java.util.Arrays.fill(out, sign);
    System.arraycopy(input, 0, out, targetLength - input.length, input.length);
    return out;
  }

  private static byte[] toFixedLenBytes(long value, int length) {
    byte[] raw = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    return signExtend(raw, length);
  }

  private static byte[] toFixedLenBytes(BigInteger value, int length) {
    byte[] raw = value.toByteArray();
    if (raw.length == length) {
      return raw;
    }
    if (raw.length == length + 1) {
      byte expectedSign = value.signum() < 0 ? (byte) 0xFF : (byte) 0x00;
      if (raw[0] == expectedSign) {
        return java.util.Arrays.copyOfRange(raw, 1, raw.length);
      }
    }
    return signExtend(raw, length);
  }

  private record TypedStats(
      Integer minI32,
      Integer maxI32,
      Long minI64,
      Long maxI64,
      Float minF32,
      Float maxF32,
      Double minF64,
      Double maxF64,
      Boolean minBool,
      Boolean maxBool,
      String minUtf8,
      String maxUtf8,
      byte[] minDecimal128Unscaled,
      byte[] maxDecimal128Unscaled,
      byte[] minDecimal256Unscaled,
      byte[] maxDecimal256Unscaled) {
    private static TypedStats empty() {
      return new TypedStats(
          null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
          null);
    }
  }
}
