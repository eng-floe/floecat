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
package ai.floedb.floecat.systemcatalog.graph;

import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Helper to build deterministic, engine-scoped system ResourceIds that can be translated back to
 * their default-engine equivalents via XOR-masked UUIDs.
 */
public final class SystemResourceIdGenerator {

  private static final String ID_PREFIX = "rid-v1";
  private static final String ENGINE_MASK_PREFIX = "engine-mask-v1";
  private static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final String HASH_ALGORITHM = "SHA-256";
  private static final ThreadLocal<MessageDigest> DIGEST =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance(HASH_ALGORITHM);
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException(HASH_ALGORITHM + " not available", e);
            }
          });
  // no cache for now
  private static final byte MARKER_BYTE0 = (byte) 0xAB;
  private static final byte MARKER_BYTE1 = (byte) 0xCD;

  private SystemResourceIdGenerator() {}

  public static String normalizeEngine(String engineKind) {
    String normalized = EngineContextNormalizer.normalizeEngineKind(engineKind);
    return normalized.isEmpty() ? EngineCatalogNames.FLOECAT_DEFAULT_CATALOG : normalized;
  }

  public static byte[] base(ResourceKind kind, String canonicalSignature) {
    MessageDigest md = DIGEST.get();
    md.reset();
    md.update(ID_PREFIX.getBytes(UTF_8));
    md.update((byte) 0);
    int kindNumber = kind == null ? 0 : kind.getNumber();
    md.update((byte) ((kindNumber >> 24) & 0xFF));
    md.update((byte) ((kindNumber >> 16) & 0xFF));
    md.update((byte) ((kindNumber >> 8) & 0xFF));
    md.update((byte) (kindNumber & 0xFF));
    md.update((byte) 0);
    if (canonicalSignature != null && !canonicalSignature.isEmpty()) {
      md.update(canonicalSignature.getBytes(UTF_8));
    }
    byte[] digest = Arrays.copyOf(md.digest(), 16);
    digest[0] = MARKER_BYTE0;
    digest[1] = MARKER_BYTE1;
    digest[6] = (byte) ((digest[6] & 0x0F) | 0x40);
    digest[8] = (byte) ((digest[8] & 0x3F) | 0x80);
    return digest;
  }

  public static byte[] mask(String engineKind) {
    String normalized = normalizeEngine(engineKind);
    return computeMask(normalized);
  }

  public static byte[] xor(byte[] left, byte[] right) {
    byte[] out = new byte[16];
    for (int i = 0; i < 16; i++) {
      out[i] = (byte) (left[i] ^ right[i]);
    }
    return out;
  }

  public static byte[] bytesFromUuid(UUID uuid) {
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }

  public static byte[] bytesFromResourceId(String id) {
    return bytesFromUuid(UUID.fromString(id));
  }

  public static UUID uuidFromBytes(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    long hi = bb.getLong();
    long lo = bb.getLong();
    return new UUID(hi, lo);
  }

  private static byte[] computeMask(String normalizedEngine) {
    MessageDigest md = DIGEST.get();
    md.reset();
    md.update(ENGINE_MASK_PREFIX.getBytes(UTF_8));
    md.update((byte) 0);
    md.update(normalizedEngine.getBytes(UTF_8));
    byte[] mask = Arrays.copyOf(md.digest(), 16);
    mask[0] = 0;
    mask[1] = 0;
    mask[6] = (byte) (mask[6] & 0x0F);
    mask[8] = (byte) (mask[8] & 0x3F);
    return mask;
  }

  public static boolean isSystemId(UUID id) {
    byte[] bytes = bytesFromUuid(id);
    return bytes[0] == MARKER_BYTE0 && bytes[1] == MARKER_BYTE1;
  }
}
