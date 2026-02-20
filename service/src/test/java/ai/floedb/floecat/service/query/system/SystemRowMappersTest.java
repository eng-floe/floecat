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

package ai.floedb.floecat.service.query.system;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.system.rpc.SystemTableRow;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import org.junit.jupiter.api.Test;

class SystemRowMappersTest {

  @Test
  void toProto_mapsAllValuesInOrder() {
    SystemObjectRow row =
        new SystemObjectRow(new Object[] {"cat", "schema", "table", "BASE TABLE"});

    SystemTableRow proto = SystemRowMappers.toProto(row);

    assertThat(proto.getValuesList()).containsExactly("cat", "schema", "table", "BASE TABLE");
  }

  @Test
  void toProto_convertsNullValuesToEmptyString() {
    SystemObjectRow row = new SystemObjectRow(new Object[] {"cat", null, "table", null});

    SystemTableRow proto = SystemRowMappers.toProto(row);

    assertThat(proto.getValuesList()).containsExactly("cat", "", "table", "");
  }

  @Test
  void toProto_handlesEmptyRow() {
    SystemObjectRow row = new SystemObjectRow(new Object[] {});

    SystemTableRow proto = SystemRowMappers.toProto(row);

    assertThat(proto.getValuesList()).isEmpty();
  }
}
