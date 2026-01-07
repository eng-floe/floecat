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

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public final class FilteringNdvProvider implements NdvProvider {
  private final Predicate<String> pathFilter;
  private final NdvProvider delegate;

  public FilteringNdvProvider(Predicate<String> pathFilter, NdvProvider delegate) {
    this.pathFilter = pathFilter;
    this.delegate = delegate;
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) throws Exception {
    if (filePath != null && pathFilter.test(filePath)) {
      delegate.contributeNdv(filePath, sinks);
    }
  }

  public static FilteringNdvProvider bySuffix(Set<String> lowercaseSuffixes, NdvProvider delegate) {
    return new FilteringNdvProvider(
        p -> {
          String s = p.toLowerCase(Locale.ROOT);
          for (String suf : lowercaseSuffixes) {
            if (s.endsWith(suf)) {
              return true;
            }
          }

          return false;
        },
        delegate);
  }
}
