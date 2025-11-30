package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

public record TablesResponse(List<TableDto> tables, PageDto page) {}
