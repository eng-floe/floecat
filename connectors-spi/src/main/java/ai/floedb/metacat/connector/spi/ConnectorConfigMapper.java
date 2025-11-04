package ai.floedb.metacat.connector.spi;

import ai.floedb.metacat.connector.rpc.Connector;

public final class ConnectorConfigMapper {
  public static ConnectorConfig fromProto(Connector c) {
    var kind =
        switch (c.getKind()) {
          case CK_ICEBERG -> ConnectorConfig.Kind.ICEBERG;
          case CK_DELTA -> ConnectorConfig.Kind.DELTA;
          case CK_GLUE -> ConnectorConfig.Kind.GLUE;
          case CK_UNITY -> ConnectorConfig.Kind.UNITY;
          default -> throw new IllegalArgumentException("unsupported kind: " + c.getKind());
        };

    var auth =
        new ConnectorConfig.Auth(
            c.getAuth().getScheme(),
            c.getAuth().getPropsMap(),
            c.getAuth().getHeaderHintsMap(),
            c.getAuth().getSecretRef());

    return new ConnectorConfig(kind, c.getDisplayName(), c.getUri(), c.getOptionsMap(), auth);
  }
}
