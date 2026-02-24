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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContextNormalizer;
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
 *
 * <p>v2: uses a 6-byte fixed marker plus RFC4122 version/variant bits to make accidental
 * false-positives from random UUIDs vanishingly unlikely (~1 / 2^54).
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

  // 6-byte marker => 48 bits, plus RFC4122 bits checked in isSystemId => ~1/2^54 false positive.
  private static final byte MARKER_BYTE0 = (byte) 0xAB;
  private static final byte MARKER_BYTE1 = (byte) 0xCD;
  private static final byte MARKER_BYTE2 = (byte) 0xEF;
  private static final byte MARKER_BYTE3 = (byte) 0x01;
  private static final byte MARKER_BYTE4 = (byte) 0x23;
  private static final byte MARKER_BYTE5 = (byte) 0x45;

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

    // Fixed marker (6 bytes).
    digest[0] = MARKER_BYTE0;
    digest[1] = MARKER_BYTE1;
    digest[2] = MARKER_BYTE2;
    digest[3] = MARKER_BYTE3;
    digest[4] = MARKER_BYTE4;
    digest[5] = MARKER_BYTE5;

    // RFC4122 version (4) and variant (2).
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

    // Preserve marker bytes across XOR so engine-scoped UUIDs still "look system".
    mask[0] = 0;
    mask[1] = 0;
    mask[2] = 0;
    mask[3] = 0;
    mask[4] = 0;
    mask[5] = 0;

    // Preserve RFC4122 version/variant bits across XOR.
    mask[6] = (byte) (mask[6] & 0x0F);
    mask[8] = (byte) (mask[8] & 0x3F);

    return mask;
  }

  public static boolean isSystemId(UUID id) {
    if (id == null) {
      return false;
    }
    byte[] b = bytesFromUuid(id);

    // 6-byte marker check
    if (b[0] != MARKER_BYTE0
        || b[1] != MARKER_BYTE1
        || b[2] != MARKER_BYTE2
        || b[3] != MARKER_BYTE3
        || b[4] != MARKER_BYTE4
        || b[5] != MARKER_BYTE5) {
      return false;
    }

    // RFC4122 bits check
    int version = (b[6] >> 4) & 0x0F;
    int variant = (b[8] & 0xC0) >> 6;
    return version == 4 && variant == 2;
  }

  public static boolean isSystemId(ResourceId id) {
    if (id == null || id.getId() == null || id.getId().isBlank()) {
      return false;
    }
    try {
      return isSystemId(UUID.fromString(id.getId()));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
