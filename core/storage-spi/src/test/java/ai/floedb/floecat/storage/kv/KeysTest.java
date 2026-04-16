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
package ai.floedb.floecat.storage.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class KeysTest {

  @Test
  void join_prepends_leading_slash() {
    assertEquals("/a/b", Keys.join("a", "b"));
  }

  @Test
  void join_single_part() {
    assertEquals("/a", Keys.join("a"));
  }

  @Test
  void join_with_empty_parts() {
    assertThrows(IllegalArgumentException.class, () -> Keys.join("", "b"));
  }

  @Test
  void prefix_appends_trailing_slash() {
    assertEquals("/a/", Keys.prefix("a"));
  }

  @Test
  void key_returns_expected_record() {
    KvStore.Key key = Keys.key("pk", "sk");
    assertEquals("pk", key.partitionKey());
    assertEquals("sk", key.sortKey());
  }

  @Test
  void join_allows_double_slashes_in_parts() {
    assertEquals("/a%2F/b", Keys.join("a/", "b"));
  }

  @Test
  void snapshotPointerByIdUsesPathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/snapshots/by-id/0000000000000000000",
        Keys.snapshotPointerById("acct id", "table id", 0L));
  }

  @Test
  void encodeSegmentUsesRfc3986PathSegmentRules() {
    assertEquals("a%20b", Keys.encodeSegment("a b"));
    assertEquals("a%2Bb", Keys.encodeSegment("a+b"));
    assertEquals("a%2Fb", Keys.encodeSegment("a/b"));
    assertEquals("a%25b", Keys.encodeSegment("a%b"));
  }

  @Test
  void joinEncodesSegments() {
    assertEquals("/a%2Fb/c%20d", Keys.join("a/b", "c d"));
  }
}
