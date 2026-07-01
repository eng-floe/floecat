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

package ai.floedb.floecat.connector.common.ndv;

import java.util.HashMap;
import java.util.Map;

public final class NdvApprox {
  public Double estimate;
  public Double rse;
  public Double ciLower;
  public Double ciUpper;
  public Double ciLevel;
  public Long rowsSeen;
  public Long rowsTotal;
  public String method;
  public Map<String, String> params = new HashMap<>();

  /** Returns an independent copy so mutations (e.g. finalizeTheta) don't alias a shared source. */
  public NdvApprox copy() {
    NdvApprox c = new NdvApprox();
    c.estimate = estimate;
    c.rse = rse;
    c.ciLower = ciLower;
    c.ciUpper = ciUpper;
    c.ciLevel = ciLevel;
    c.rowsSeen = rowsSeen;
    c.rowsTotal = rowsTotal;
    c.method = method;
    c.params = new HashMap<>(params);
    return c;
  }
}
