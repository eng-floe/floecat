package ai.floedb.floecat.connector.auth;

import ai.floedb.floecat.connector.spi.AuthProvider;
import java.util.HashMap;
import java.util.Map;

public final class AwsSigV4AuthProvider implements AuthProvider {
  private final String signingName;
  private final String signingRegion;

  public AwsSigV4AuthProvider(String signingName, String signingRegion) {
    this.signingName = signingName;
    this.signingRegion = signingRegion;
  }

  @Override
  public String scheme() {
    return "aws-sigv4";
  }

  @Override
  public Map<String, String> apply(Map<String, String> baseProps) {
    var p = new HashMap<>(baseProps);
    p.put("rest.auth.type", "sigv4");
    p.put("rest.signing-name", signingName);
    p.put("rest.signing-region", signingRegion);
    p.putIfAbsent("s3.region", signingRegion);
    return p;
  }
}
