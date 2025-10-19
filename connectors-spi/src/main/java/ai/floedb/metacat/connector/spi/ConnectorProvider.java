package ai.floedb.metacat.connector.spi;

public interface ConnectorProvider {
  String kind();
  MetacatConnector create(ConnectorConfig config);
}
