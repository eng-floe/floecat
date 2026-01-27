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

package ai.floedb.floecat.systemcatalog.def;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import org.junit.jupiter.api.Test;

class SystemTableDefTest {

  private static final List<SystemColumnDef> COLUMNS = List.of();

  @Test
  void storageWithoutPathFails() {
    assertThatThrownBy(
            () ->
                new SystemTableDef(
                    NameRefUtil.name("namespace", "t"),
                    "t",
                    COLUMNS,
                    TableBackendKind.TABLE_BACKEND_KIND_STORAGE,
                    "scanner",
                    "",
                    "",
                    List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("storagePath");
  }

  @Test
  void engineWithoutLabelFails() {
    assertThatThrownBy(
            () ->
                new SystemTableDef(
                    NameRefUtil.name("namespace", "t"),
                    "t",
                    COLUMNS,
                    TableBackendKind.TABLE_BACKEND_KIND_ENGINE,
                    "scanner",
                    "",
                    "",
                    List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("engineLabel");
  }

  @Test
  void floecatWithoutScannerFails() {
    assertThatThrownBy(
            () ->
                new SystemTableDef(
                    NameRefUtil.name("namespace", "t"),
                    "t",
                    COLUMNS,
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "",
                    "",
                    "",
                    List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("scannerId");
  }
}
