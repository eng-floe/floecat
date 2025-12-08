package ai.floedb.floecat.catalog.system_objects.spi;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.stream.Stream;

/** Result returned by MetadataGraph.scanSystemObject(). */
public record SystemObjectScanResult(SchemaColumn[] schema, Stream<SystemObjectRow> rows) {}
