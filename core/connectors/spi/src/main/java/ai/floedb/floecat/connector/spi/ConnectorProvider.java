package ai.floedb.floecat.connector.spi;

public interface ConnectorProvider {
  String kind();

  FloecatConnector create(ConnectorConfig config);
}
