package ai.floedb.metacat.client.trino;

import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import java.util.List;

public final class MetacatSplit implements ConnectorSplit {
  private final ScanFile dataFile;
  private final List<ScanFile> deleteFiles;
  private final SchemaDescriptor schema;

  public MetacatSplit(ScanFile dataFile, List<ScanFile> deleteFiles, SchemaDescriptor schema) {
    this.dataFile = dataFile;
    this.deleteFiles = List.copyOf(deleteFiles);
    this.schema = schema;
  }

  public ScanFile dataFile() {
    return dataFile;
  }

  public List<ScanFile> deleteFiles() {
    return deleteFiles;
  }

  public SchemaDescriptor schema() {
    return schema;
  }

  public String path() {
    return dataFile.getFilePath();
  }

  public long size() {
    return dataFile.getFileSizeInBytes();
  }

  @Override
  public boolean isRemotelyAccessible() {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses() {
    return List.of();
  }

  @Override
  public SplitWeight getSplitWeight() {
    return SplitWeight.standard();
  }

  @Override
  public long getRetainedSizeInBytes() {
    return dataFile.getSerializedSize();
  }

  @Override
  public java.util.Map<String, String> getSplitInfo() {
    return java.util.Map.of();
  }
}
