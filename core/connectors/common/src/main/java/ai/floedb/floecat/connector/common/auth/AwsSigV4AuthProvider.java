/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.connector.common.auth;

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
