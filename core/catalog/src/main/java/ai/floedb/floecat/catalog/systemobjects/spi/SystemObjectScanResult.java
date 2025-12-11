package ai.floedb.floecat.catalog.systemobjects.spi;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.stream.Stream;

/** Result returned by MetadataGraph.scanSystemObject(). */
public record SystemObjectScanResult(SchemaColumn[] schema, Stream<SystemObjectRow> rows) {}
