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

package ai.floedb.floecat.service.telemetry;

import jakarta.enterprise.context.ApplicationScoped;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

/** Periodically exports bounded gRPC payload byte counts as a custom Pyroscope profile. */
@ApplicationScoped
public class NetworkProfileExporter {
  private static final Logger LOG = Logger.getLogger(NetworkProfileExporter.class);
  private static final AtomicLong INBOUND_BYTES = new AtomicLong();
  private static final AtomicLong OUTBOUND_BYTES = new AtomicLong();
  private static final HttpClient CLIENT =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
  private static final String SAMPLE_TYPE_CONFIG =
      "{\"network_bytes\":{\"units\":\"bytes\",\"aggregation\":\"sum\","
          + "\"display-name\":\"network_bytes\",\"sampled\":false}}";

  static void recordInbound(long bytes) {
    if (bytes > 0 && endpoint() != null) INBOUND_BYTES.addAndGet(bytes);
  }

  static void recordOutbound(long bytes) {
    if (bytes > 0 && endpoint() != null) OUTBOUND_BYTES.addAndGet(bytes);
  }

  @io.quarkus.scheduler.Scheduled(every = "${floecat.network-profile.interval:60s}")
  void export() {
    String endpoint = endpoint();
    if (endpoint == null) return;

    long inbound = INBOUND_BYTES.getAndSet(0);
    long outbound = OUTBOUND_BYTES.getAndSet(0);
    if (inbound == 0 && outbound == 0) return;

    Instant until = Instant.now();
    Instant from = until.minusSeconds(60);
    String boundary = "floedb-floecat-network-" + until.toEpochMilli();
    try {
      URI uri = ingestUri(endpoint, from.getEpochSecond(), until.getEpochSecond());
      byte[] body = multipart(boundary, pprof(inbound, outbound, from, until));
      HttpResponse<Void> response =
          CLIENT.send(
              HttpRequest.newBuilder(uri)
                  .timeout(Duration.ofSeconds(8))
                  .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                  .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                  .build(),
              HttpResponse.BodyHandlers.discarding());
      if (response.statusCode() / 100 != 2) {
        throw new IOException("Pyroscope returned HTTP " + response.statusCode());
      }
    } catch (Exception e) {
      INBOUND_BYTES.addAndGet(inbound);
      OUTBOUND_BYTES.addAndGet(outbound);
      LOG.warn("Floecat network profile upload failed", e);
    }
  }

  static URI ingestUri(String endpoint, long from, long until) {
    URI base = URI.create(endpoint);
    if ((!"http".equalsIgnoreCase(base.getScheme()) && !"https".equalsIgnoreCase(base.getScheme()))
        || base.getHost() == null) {
      throw new IllegalArgumentException("Pyroscope endpoint must use HTTP(S)");
    }
    String path = base.getPath() == null ? "" : base.getPath().replaceAll("/+$", "");
    if (!path.endsWith("/ingest")) path += "/ingest";
    String query =
        "name="
            + enc("floecat-service{service_name=floecat-service}")
            + "&from="
            + from
            + "&until="
            + until;
    return URI.create(base.getScheme() + "://" + base.getRawAuthority() + path + "?" + query);
  }

  private static String endpoint() {
    String configured = System.getenv("FLOECAT_PYROSCOPE_ENDPOINT");
    if (configured == null || configured.isBlank()) {
      configured = System.getenv("FLOESCAN_PYROSCOPE_ENDPOINT");
    }
    return configured == null || configured.isBlank() ? null : configured;
  }

  private static byte[] multipart(String boundary, byte[] profile) throws IOException {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    part(body, boundary, "profile", "profile.pprof", "application/octet-stream", profile);
    part(
        body,
        boundary,
        "sample_type_config",
        "sample_type_config.json",
        "application/json",
        SAMPLE_TYPE_CONFIG.getBytes(StandardCharsets.UTF_8));
    body.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
    return body.toByteArray();
  }

  private static void part(
      ByteArrayOutputStream out,
      String boundary,
      String name,
      String file,
      String type,
      byte[] data)
      throws IOException {
    out.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
    out.write(
        ("Content-Disposition: form-data; name=\"" + name + "\"; filename=\"" + file + "\"\r\n")
            .getBytes(StandardCharsets.UTF_8));
    out.write(("Content-Type: " + type + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
    out.write(data);
    out.write("\r\n".getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] pprof(long inbound, long outbound, Instant from, Instant until)
      throws IOException {
    ByteArrayOutputStream profile = new ByteArrayOutputStream();
    bytes(profile, 1, valueType());
    if (inbound > 0) bytes(profile, 2, sample(inbound, 1, 2, 3));
    if (outbound > 0) bytes(profile, 2, sample(outbound, 1, 2, 4));
    bytes(profile, 4, location(1, 1));
    bytes(profile, 4, location(2, 2));
    bytes(profile, 4, location(3, 3));
    bytes(profile, 4, location(4, 4));
    bytes(profile, 5, function(1, 3));
    bytes(profile, 5, function(2, 4));
    bytes(profile, 5, function(3, 5));
    bytes(profile, 5, function(4, 6));
    string(profile, 6, "");
    string(profile, 6, "network_bytes");
    string(profile, 6, "bytes");
    string(profile, 6, "floecat");
    string(profile, 6, "grpc");
    string(profile, 6, "inbound");
    string(profile, 6, "outbound");
    integer(profile, 9, from.toEpochMilli() * 1_000_000L);
    integer(profile, 10, Duration.between(from, until).toNanos());
    integer(profile, 14, 1);
    return profile.toByteArray();
  }

  private static byte[] valueType() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    integer(out, 1, 1);
    integer(out, 2, 2);
    return out.toByteArray();
  }

  private static byte[] sample(long value, long... locationIds) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (long locationId : locationIds) integer(out, 1, locationId);
    integer(out, 2, value);
    return out.toByteArray();
  }

  private static byte[] location(long id, long functionId) throws IOException {
    ByteArrayOutputStream line = new ByteArrayOutputStream();
    integer(line, 1, functionId);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    integer(out, 1, id);
    bytes(out, 4, line.toByteArray());
    return out.toByteArray();
  }

  private static byte[] function(long id, long nameId) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    integer(out, 1, id);
    integer(out, 2, nameId);
    integer(out, 3, nameId);
    return out.toByteArray();
  }

  private static void string(ByteArrayOutputStream out, int field, String value)
      throws IOException {
    bytes(out, field, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void bytes(ByteArrayOutputStream out, int field, byte[] value) throws IOException {
    varint(out, ((long) field << 3) | 2);
    varint(out, value.length);
    out.write(value);
  }

  private static void integer(ByteArrayOutputStream out, int field, long value) throws IOException {
    varint(out, (long) field << 3);
    varint(out, value);
  }

  private static void varint(ByteArrayOutputStream out, long value) throws IOException {
    while ((value & ~0x7fL) != 0) {
      out.write((int) (value & 0x7f) | 0x80);
      value >>>= 7;
    }
    out.write((int) value);
  }

  private static String enc(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
