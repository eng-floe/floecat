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

package ai.floedb.floecat.service.metagraph.loader;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.NameRef;
import org.junit.jupiter.api.Test;

class NodeLoaderTest {

  @Test
  void parseFqn_nameOnly() {
    NameRef ref = NodeLoader.parseFqn("tbl");
    assertEquals("tbl", ref.getName());
    assertEquals("", ref.getCatalog());
    assertEquals(0, ref.getPathCount());
  }

  @Test
  void parseFqn_catalogAndName() {
    NameRef ref = NodeLoader.parseFqn("cat.tbl");
    assertEquals("cat", ref.getCatalog());
    assertEquals(0, ref.getPathCount());
    assertEquals("tbl", ref.getName());
  }

  @Test
  void parseFqn_catalogPathAndName() {
    NameRef ref = NodeLoader.parseFqn("cat.sales.tbl");
    assertEquals("cat", ref.getCatalog());
    assertEquals(1, ref.getPathCount());
    assertEquals("sales", ref.getPath(0));
    assertEquals("tbl", ref.getName());
  }

  @Test
  void parseFqn_catalogTwoPathsAndName() {
    NameRef ref = NodeLoader.parseFqn("cat.ns1.ns2.tbl");
    assertEquals("cat", ref.getCatalog());
    assertEquals(2, ref.getPathCount());
    assertEquals("ns1", ref.getPath(0));
    assertEquals("ns2", ref.getPath(1));
    assertEquals("tbl", ref.getName());
  }
}
