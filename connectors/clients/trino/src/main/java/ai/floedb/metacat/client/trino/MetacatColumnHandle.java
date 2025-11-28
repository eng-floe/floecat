package ai.floedb.metacat.client.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

public record MetacatColumnHandle(String name, Type type) implements ColumnHandle {}
