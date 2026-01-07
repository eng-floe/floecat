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

package ai.floedb.floecat.service.util;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

public final class PagingTestUtil {

  /** Minimal page chunk view for assertions. */
  public static final class PageChunk<T> {
    public final List<T> items;
    public final String nextToken;
    public final int totalSize;

    public PageChunk(List<T> items, String nextToken, int totalSize) {
      this.items = Objects.requireNonNull(items);
      this.nextToken = nextToken == null ? "" : nextToken;
      this.totalSize = totalSize;
    }
  }

  @FunctionalInterface
  public interface GrpcPager<T> extends BiFunction<Integer, String, PageChunk<T>> {
    @Override
    PageChunk<T> apply(Integer pageSize, String token);
  }

  public static <T> void assertBasicTwoPageFlow(GrpcPager<T> pager, int limit) {
    PageChunk<T> p1 = pager.apply(limit, "");
    if (p1.items.size() != limit) {
      throw new AssertionError("first page should return LIMIT items; got " + p1.items.size());
    }
    if (p1.nextToken.isBlank()) {
      throw new AssertionError("first page should return a non-empty next_page_token");
    }

    PageChunk<T> p2 = pager.apply(limit, p1.nextToken);
    if (p2.totalSize != p1.totalSize) {
      throw new AssertionError("total_size should remain constant across pages");
    }
  }
}
