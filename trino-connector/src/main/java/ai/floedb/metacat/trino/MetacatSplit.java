package ai.floedb.metacat.trino;

import ai.floedb.metacat.planning.rpc.PlanFile;
import ai.floedb.metacat.planning.rpc.SchemaDescriptor;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import java.util.List;

/**
 * Connector split that carries a single data file plus any associated delete files (Iceberg-style)
 * and the projected schema descriptor.
 */
public final class MetacatSplit implements ConnectorSplit {
  private final PlanFile dataFile;
  private final List<PlanFile> deleteFiles;
  private final SchemaDescriptor schema;

  public MetacatSplit(PlanFile dataFile, List<PlanFile> deleteFiles, SchemaDescriptor schema) {
    this.dataFile = dataFile;
    this.deleteFiles = List.copyOf(deleteFiles);
    this.schema = schema;
  }

  public PlanFile dataFile() {
    return dataFile;
  }

  public List<PlanFile> deleteFiles() {
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
