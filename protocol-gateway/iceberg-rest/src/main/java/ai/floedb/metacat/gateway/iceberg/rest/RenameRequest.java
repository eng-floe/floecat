package ai.floedb.metacat.gateway.iceberg.rest;

import java.util.List;

public record RenameRequest(TableIdentifierBody source, TableIdentifierBody destination) {
  public record TableIdentifierBody(List<String> namespace, String name) {}
}
