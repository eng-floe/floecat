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

package ai.floedb.floecat.scanner.spi;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsProviderTest {

  @Test
  void targetStatsPageNormalizesInputs() {
    List<TargetStatsRecord> mutable = new ArrayList<>();
    mutable.add(TargetStatsRecord.getDefaultInstance());
    StatsProvider.TargetStatsPage page = new StatsProvider.TargetStatsPage(mutable, null);
    mutable.clear();

    assertThat(page.nextToken()).isEmpty();
    assertThat(page.items()).hasSize(1);
  }

  @Test
  void defaultListPersistedStatsReturnsEmptyPage() {
    StatsProvider provider = StatsProvider.NONE;

    StatsProvider.TargetStatsPage page =
        provider.listPersistedStats(
            ResourceId.getDefaultInstance(), 0L, Optional.of("TABLE"), 100, "");

    assertThat(page).isEqualTo(StatsProvider.TargetStatsPage.EMPTY);
  }
}
