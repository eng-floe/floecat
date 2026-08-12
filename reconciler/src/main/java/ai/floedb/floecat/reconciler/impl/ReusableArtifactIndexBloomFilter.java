/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/** Encoding and membership logic for reusable-artifact run Bloom filters. */
final class ReusableArtifactIndexBloomFilter {
  private static final int MAX_BYTES = 16 * 1024 * 1024;
  private static final int BITS_PER_ENTRY = 20;
  private static final int MAX_HASHES = 14;
  private static final int MIN_BITS = 1024;

  private final int bitCount;
  private final int hashes;
  private final int entryCount;
  private final byte[] bits;

  private ReusableArtifactIndexBloomFilter(int bitCount, int hashes, int entryCount, byte[] bits) {
    this.bitCount = bitCount;
    this.hashes = hashes;
    this.entryCount = entryCount;
    this.bits = bits;
  }

  static ReusableArtifactIndexBloomFilter create(List<byte[]> digests) {
    ReusableArtifactIndexBloomFilter filter = create(digests.size());
    digests.forEach(filter::add);
    return filter;
  }

  static ReusableArtifactIndexBloomFilter create(int entryCount) {
    int bitCount = bitCount(entryCount);
    int hashes =
        Math.max(
            1,
            Math.min(
                MAX_HASHES,
                (int) Math.round(((double) bitCount / (double) entryCount) * Math.log(2.0d))));
    return new ReusableArtifactIndexBloomFilter(
        bitCount, hashes, entryCount, new byte[Math.ceilDiv(bitCount, 8)]);
  }

  static ReusableArtifactIndexBloomFilter parse(byte[] bytes, long expectedEntries) {
    if (bytes == null || bytes.length < 9) {
      throw new IllegalArgumentException("reusable artifact Bloom filter is invalid");
    }
    ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    int bitCount = input.getInt();
    int hashes = Byte.toUnsignedInt(input.get());
    int entryCount = input.getInt();
    int bitBytes = bitCount > 0 ? Math.ceilDiv(bitCount, 8) : -1;
    if (bitCount <= 0
        || bitCount % 64 != 0
        || hashes <= 0
        || hashes > MAX_HASHES
        || bitCount > MAX_BYTES * Byte.SIZE
        || input.remaining() != bitBytes
        || Integer.toUnsignedLong(entryCount) != expectedEntries) {
      throw new IllegalArgumentException("reusable artifact Bloom filter shape is invalid");
    }
    byte[] bits = new byte[bitBytes];
    input.get(bits);
    return new ReusableArtifactIndexBloomFilter(bitCount, hashes, entryCount, bits);
  }

  static int bitCount(int entryCount) {
    if (entryCount <= 0) {
      throw new IllegalArgumentException("reusable artifact Bloom filter entry count is invalid");
    }
    long desiredBits =
        Math.max((long) MIN_BITS, Math.multiplyExact((long) entryCount, BITS_PER_ENTRY));
    return Math.toIntExact(
        Math.ceilDiv(Math.min(desiredBits, (long) MAX_BYTES * Byte.SIZE), 64L) * 64L);
  }

  byte[] bytes() {
    return ByteBuffer.allocate(9 + bits.length)
        .order(ByteOrder.BIG_ENDIAN)
        .putInt(bitCount)
        .put((byte) hashes)
        .putInt(entryCount)
        .put(bits)
        .array();
  }

  void add(byte[] digest) {
    long first = longAt(digest, 0);
    long second = longAt(digest, 8) | 1L;
    for (int index = 0; index < hashes; index++) {
      int bit = (int) Long.remainderUnsigned(first + index * second, bitCount);
      bits[bit >>> 3] |= (byte) (1 << (bit & 7));
    }
  }

  boolean mightContain(byte[] digest) {
    long first = longAt(digest, 0);
    long second = longAt(digest, 8) | 1L;
    for (int index = 0; index < hashes; index++) {
      int bit = (int) Long.remainderUnsigned(first + index * second, bitCount);
      if ((bits[bit >>> 3] & (1 << (bit & 7))) == 0) {
        return false;
      }
    }
    return true;
  }

  private static long longAt(byte[] digest, int offset) {
    return ByteBuffer.wrap(digest, offset, Long.BYTES).order(ByteOrder.BIG_ENDIAN).getLong();
  }
}
