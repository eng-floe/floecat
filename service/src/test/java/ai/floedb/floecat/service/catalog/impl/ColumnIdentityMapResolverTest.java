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

package ai.floedb.floecat.service.catalog.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMapRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaNode;
import ai.floedb.floecat.service.cache.ObjectCache;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class ColumnIdentityMapResolverTest {
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("table")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  @Test
  void publicationValidationIsTableScopedAndDoesNotReadTheBlob() {
    BlobStore blobs = mock(BlobStore.class);
    var resolver =
        new ColumnIdentityMapResolver(blobs, BlobCacheAccess.disabled(), ObjectCache.forTesting());
    ColumnIdentityMap map = identityMap();
    var serialized = ColumnIdentityMaps.serialize(map);
    String uri =
        Keys.columnIdentityBlobUri(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), serialized.sha256Hex());
    ColumnIdentityMapRef ref = serialized.reference(uri);

    resolver.validateReference(TABLE_ID, ref);

    verifyNoInteractions(blobs);
    ResourceId otherTable = TABLE_ID.toBuilder().setId("other-table").build();
    assertThatThrownBy(() -> resolver.validateReference(otherTable, ref))
        .hasMessageContaining("does not match its table and digest");
    verifyNoInteractions(blobs);
  }

  @Test
  void bytesReturnsTheExactValidatedWireRepresentation() throws Exception {
    ColumnIdentityMap map = identityMap();
    byte[] canonical = map.toByteArray();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    CodedOutputStream coded = CodedOutputStream.newInstance(output);
    coded.writeUInt32(100, 7);
    coded.flush();
    output.writeBytes(canonical);
    byte[] stored = output.toByteArray();
    assertThat(ColumnIdentityMap.parseFrom(stored).toByteArray()).isNotEqualTo(stored);

    byte[] digest = MessageDigest.getInstance("SHA-256").digest(stored);
    String uri =
        Keys.columnIdentityBlobUri(
            TABLE_ID.getAccountId(), TABLE_ID.getId(), HexFormat.of().formatHex(digest));
    ColumnIdentityMapRef ref =
        ColumnIdentityMaps.reference(uri, map).toBuilder()
            .setPayloadBytes(stored.length)
            .setPayloadSha256(ByteString.copyFrom(digest))
            .build();
    var blobs = new InMemoryBlobStore();
    blobs.putImmutable(uri, stored, "application/x-protobuf");
    var resolver =
        new ColumnIdentityMapResolver(blobs, BlobCacheAccess.disabled(), ObjectCache.forTesting());

    assertThat(resolver.bytes(TABLE_ID, ref)).isEqualTo(stored);
  }

  private static ColumnIdentityMap identityMap() {
    SchemaNode node =
        new SchemaNode(
            ColumnPath.ROOT.field("value"), 1, true, OptionalInt.empty(), Optional.empty());
    return ColumnIdentityMaps.toProto(
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of(node)),
            7L,
            IdentityMode.STRUCTURED_PATH,
            Optional.empty(),
            0L,
            PreferredCanonicalIds.NONE,
            HistoryTier.VERSIONED_LOG));
  }
}
