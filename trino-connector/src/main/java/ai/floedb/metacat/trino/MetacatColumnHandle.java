package ai.floedb.metacat.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

public record MetacatColumnHandle(String name, Type type) implements ColumnHandle {}
