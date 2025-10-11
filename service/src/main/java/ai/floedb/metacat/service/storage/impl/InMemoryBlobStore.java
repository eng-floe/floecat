package ai.floedb.metacat.service.storage.impl;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.arc.properties.IfBuildProperty;

import ai.floedb.metacat.common.rpc.BlobHeader;
import ai.floedb.metacat.common.rpc.Tag;
import ai.floedb.metacat.service.storage.BlobStore;

@ApplicationScoped
@IfBuildProperty(name = "metacat.blob", stringValue = "memory")
public class InMemoryBlobStore implements BlobStore {

  private static final String TAG_CONTENT_TYPE   = "contentType";
  private static final String TAG_CONTENT_LENGTH = "contentLength";
  private static final String TAG_LAST_MODIFIED  = "lastModified";

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
    Objects.requireNonNull(uri, "uri");
    Objects.requireNonNull(bytes, "bytes");
    if (contentType == null || contentType.isBlank()) contentType = "application/octet-stream";

    byte[] copy = Arrays.copyOf(bytes, bytes.length);

    String etag = sha256B64(copy);
    long now = clock.millis();
    BlobHeader.Builder hb = BlobHeader.newBuilder()
      .setSchemaVersion("v1")
      .setEtag(etag)
      .setCreatedAt(Timestamps.fromMillis(now));

    addTag(hb, TAG_CONTENT_TYPE, contentType);
    addTag(hb, TAG_CONTENT_LENGTH, Integer.toString(copy.length));
    addTag(hb, TAG_LAST_MODIFIED, Long.toString(now));

    map.put(uri, new Blob(copy, hb.build()));
  }

  @Override
  public byte[] get(String uri) {
    Blob b = map.get(uri);
    if (b == null) throw new IllegalArgumentException("Blob not found: " + uri);
    return Arrays.copyOf(b.data, b.data.length);
  }

  @Override
  public Optional<BlobHeader> head(String uri) {
    return Optional.ofNullable(map.get(uri)).map(bl -> bl.hdr);
  }

  @Override
  public boolean delete(String uri) {
    return map.remove(uri) != null;
  }

  @Override
  public Map<String, byte[]> getBatch(List<String> uris) {
    if (uris == null || uris.isEmpty()) return Collections.emptyMap();
    Map<String, byte[]> out = new HashMap<>(uris.size());
    for (String u : uris) {
      Blob b = map.get(u);
      if (b != null) out.put(u, Arrays.copyOf(b.data, b.data.length));
    }
    return out;
  }

  private static void addTag(BlobHeader.Builder hb, String key, String value) {
    hb.addTags(Tag.newBuilder().setKey(key).setValue(value).build());
  }

  public static Optional<String> getTag(BlobHeader hdr, String key) {
    for (Tag t : hdr.getTagsList()) {
      if (key.equals(t.getKey())) return Optional.ofNullable(t.getValue());
    }
    return Optional.empty();
  }

  private static String sha256B64(byte[] data) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(data);
      return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
