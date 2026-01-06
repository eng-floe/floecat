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

package ai.floedb.floecat.storage;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.Tag;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Singleton;
import java.time.Clock;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@IfBuildProperty(name = "floecat.blob", stringValue = "memory")
public class InMemoryBlobStore implements BlobStore {
  final String TAG_CONTENT_TYPE = "contentType";
  private final Clock clock = Clock.systemUTC();

  private static final class Blob {
    final byte[] data;
    final BlobHeader hdr;

    Blob(byte[] d, BlobHeader h) {
      this.data = d;
      this.hdr = h;
    }
  }

  private final Map<String, Blob> map = new ConcurrentHashMap<>();

  @Override
  public void put(String uri, byte[] bytes, String contentType) {
    uri = normalize(uri);
    Objects.requireNonNull(uri, "uri");
    Objects.requireNonNull(bytes, "bytes");
    final String ct =
        (contentType == null || contentType.isBlank()) ? "application/octet-stream" : contentType;

    final byte[] copy = Arrays.copyOf(bytes, bytes.length);
    final String etag = sha256B64(copy);
    final long now = clock.millis();

    map.compute(
        uri,
        (k, prev) -> {
          final BlobHeader prevHdr = prev == null ? null : prev.hdr;
          final Timestamp createdAt =
              prevHdr == null ? Timestamps.fromMillis(now) : prevHdr.getCreatedAt();

          BlobHeader.Builder hb =
              BlobHeader.newBuilder()
                  .setSchemaVersion("v1")
                  .setEtag(etag)
                  .setCreatedAt(createdAt)
                  .setContentLength(copy.length)
                  .setLastModifiedAt(Timestamps.fromMillis(now));

          addTag(hb, TAG_CONTENT_TYPE, ct);

          return new Blob(copy, hb.build());
        });
  }

  @Override
  public byte[] get(String uri) {
    uri = normalize(uri);
    Blob b = map.get(uri);
    if (b == null) {
      return null;
    }
    return Arrays.copyOf(b.data, b.data.length);
  }

  @Override
  public Optional<BlobHeader> head(String uri) {
    uri = normalize(uri);
    return Optional.ofNullable(map.get(uri)).map(bl -> bl.hdr);
  }

  @Override
  public boolean delete(String uri) {
    uri = normalize(uri);
    return map.remove(uri) != null;
  }

  @Override
  public Map<String, byte[]> getBatch(List<String> uris) {
    if (uris == null || uris.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, byte[]> out = new HashMap<>(uris.size());
    for (String u : uris) {
      String k = normalize(u);
      Blob b = map.get(k);
      if (b != null) {
        out.put(u, Arrays.copyOf(b.data, b.data.length));
      }
    }

    return out;
  }

  private static void addTag(BlobHeader.Builder hb, String key, String value) {
    hb.addTags(Tag.newBuilder().setKey(key).setValue(value).build());
  }

  public static Optional<String> getTag(BlobHeader hdr, String key) {
    for (Tag t : hdr.getTagsList()) {
      if (key.equals(t.getKey())) {
        return Optional.ofNullable(t.getValue());
      }
    }
    return Optional.empty();
  }

  @Override
  public void deletePrefix(String prefix) {
    final String p = normalize(prefix);
    var it = map.keySet().iterator();
    while (it.hasNext()) {
      String k = it.next();
      if (k.startsWith(p)) {
        it.remove();
      }
    }
  }

  private static final class PageImpl implements BlobStore.Page {
    private final List<String> keys;
    private final String next;

    PageImpl(List<String> keys, String next) {
      this.keys = keys;
      this.next = next;
    }

    @Override
    public List<String> keys() {
      return keys;
    }

    @Override
    public String nextToken() {
      return next;
    }
  }

  @Override
  public BlobStore.Page list(String prefix, int limit, String pageToken) {
    final String p = normalize(prefix);
    final int lim = Math.max(1, limit);

    var keys = map.keySet().stream().filter(k -> k.startsWith(p)).sorted().toList();

    int startIdx = 0;
    if (pageToken != null && !pageToken.isBlank()) {
      int idx = Collections.binarySearch(keys, pageToken);
      startIdx = (idx >= 0) ? (idx + 1) : Math.max(0, -idx - 1);
    }

    if (startIdx >= keys.size()) {
      return new PageImpl(List.of(), "");
    }

    int endIdx = Math.min(keys.size(), startIdx + lim);
    var slice = keys.subList(startIdx, endIdx);

    String next = (endIdx < keys.size()) ? slice.get(slice.size() - 1) : "";
    return new PageImpl(List.copyOf(slice), next);
  }

  private static String sha256B64(byte[] data) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(data);
      return Base64.getEncoder().encodeToString(digest);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String normalize(String key) {
    return key == null ? "" : (key.startsWith("/") ? key.substring(1) : key);
  }
}
