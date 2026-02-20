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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for pg_catalog.pg_type scanner.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Engine-specific payload decoding
 *   <li>SYSTEM objects require persisted hints (no fallback)
 *   <li>Default pg_catalog namespace semantics
 * </ul>
 *
 * <p>NOTE: There are currently no USER types; tests must not create user TypeNodes.
 */
final class PgTypeScannerTest {

  private static final EngineContext ENGINE_CTX = EngineContext.of("floedb", "1.0");

  private final PgTypeScanner scanner = new PgTypeScanner();

  @Test
  void scan_usesEngineSpecificPayload_whenPresent() {
    TypeNode type =
        systemType(
            "int4",
            Map.of(
                new EngineHintKey("floedb", "1.0", TYPE.type()),
                new EngineHint(
                    TYPE.type(),
                    FloeTypeSpecific.newBuilder()
                        .setOid(23)
                        .setTypname("int4")
                        .setTypnamespace(11)
                        .setTyplen(4)
                        .setTypbyval(true)
                        .setTypdelim(",")
                        .setTypelem(0)
                        .setTyparray(0)
                        .setTypalign("i")
                        .build()
                        .toByteArray())));

    SystemObjectScanContext ctx = contextWith(type);

    SystemObjectRow row = scanner.scan(ctx).findFirst().orElseThrow();
    Object[] v = row.values();

    assertThat(v[0]).isEqualTo(23);
    assertThat(v[1]).isEqualTo("int4");
    assertThat(v[2]).isEqualTo(11);
    assertThat(v[3]).isEqualTo(4);
    assertThat(v[4]).isEqualTo(true);
    assertThat(v[5]).isEqualTo(",");
    assertThat(v[6]).isEqualTo(0);
    assertThat(v[7]).isEqualTo(0);
    assertThat(v[8]).isEqualTo("i");
  }

  @Test
  void scan_throws_whenSystemPayloadMissing() {
    TypeNode type = systemType("custom_type", Map.of());

    SystemObjectScanContext ctx = contextWith(type);

    assertThatThrownBy(() -> scanner.scan(ctx).findFirst().orElseThrow())
        .isInstanceOf(MissingSystemOidException.class);
  }

  // ----------------------------------------------------------------------
  // Fixtures
  // ----------------------------------------------------------------------

  private static SystemObjectScanContext contextWith(TypeNode... types) {
    TestCatalogOverlay overlay = new TestCatalogOverlay();
    for (TypeNode t : types) {
      overlay.addNode(t);
    }
    return new SystemObjectScanContext(overlay, null, PgCatalogTestIds.catalog(), ENGINE_CTX);
  }

  private static TypeNode systemType(String name, Map<EngineHintKey, EngineHint> engineHints) {
    return new TypeNode(
        PgCatalogTestIds.type(asNameRef(name)),
        1,
        Instant.EPOCH,
        "15",
        name,
        "U",
        false,
        null,
        engineHints);
  }

  private static NameRef asNameRef(String qualified) {
    if (qualified == null || qualified.isBlank()) {
      return NameRef.getDefaultInstance();
    }
    String[] parts = qualified.split("\\.");
    return NameRefUtil.name(parts);
  }
}
