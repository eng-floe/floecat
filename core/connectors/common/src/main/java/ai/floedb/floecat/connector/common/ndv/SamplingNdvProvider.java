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

import java.util.Map;
import java.util.Objects;
import java.util.Random;

public final class SamplingNdvProvider implements NdvProvider {
  private final NdvProvider delegate;
  private final double fraction;
  private final long maxFiles;
  private long seen = 0;
  private final Random rnd;

  public SamplingNdvProvider(NdvProvider delegate, double fraction, long maxFiles) {
    this.delegate = Objects.requireNonNull(delegate, "delegate NDV provider required");

    if (fraction <= 0.0 || fraction > 1.0) {
      this.fraction = 1.0;
    } else {
      this.fraction = fraction;
    }

    this.maxFiles = Math.max(0, maxFiles);

    this.rnd = new Random(1L);
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) throws Exception {
    seen++;

    if (maxFiles > 0 && seen > maxFiles) {
      return;
    }

    if (fraction < 1.0 && rnd.nextDouble() > fraction) {
      return;
    }

    delegate.contributeNdv(filePath, sinks);
  }
}
