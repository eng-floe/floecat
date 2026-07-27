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

package ai.floedb.floecat.tools.transfer;

import ai.floedb.floecat.connector.rpc.ConnectorTransferBundle;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

final class ConnectorArchive {
  static final int FORMAT_VERSION = 1;
  private static final int MAX_BUNDLE_BYTES = 64 * 1024 * 1024;

  private ConnectorArchive() {}

  static void write(Path output, ConnectorTransferBundle bundle, boolean force) throws IOException {
    Path absolute = output.toAbsolutePath();
    if (Files.exists(absolute) && !force) {
      throw new IOException("output already exists (use --force): " + absolute);
    }
    Path parent = absolute.getParent();
    if (parent != null) Files.createDirectories(parent);
    Path temporary = Files.createTempFile(parent, ".floecat-connectors-", ".tmp");
    try {
      setOwnerOnly(temporary);
      try (var zip = new ZipOutputStream(Files.newOutputStream(temporary))) {
        put(zip, "bundle.pb", bundle.toByteArray());
        put(zip, "manifest.json", manifest(bundle).getBytes(StandardCharsets.UTF_8));
        for (var entry : bundle.getEntriesList()) {
          String base =
              "connectors/" + safeSegment(entry.getConnector().getResourceId().getId()) + "/";
          put(zip, base + "connector.pb", entry.getConnector().toByteArray());
          put(
              zip,
              base + "portable-spec.json",
              protobufJson(entry.getPortableSpec()).getBytes(StandardCharsets.UTF_8));
          if (entry.hasCredentials()) {
            put(zip, base + "credentials.pb", entry.getCredentials().toByteArray());
          }
        }
      }
      if (force) {
        Files.move(
            temporary,
            absolute,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
      } else {
        Files.move(temporary, absolute, StandardCopyOption.ATOMIC_MOVE);
      }
      setOwnerOnly(absolute);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  static ConnectorTransferBundle read(Path input) throws IOException {
    try (var zip = new ZipFile(input.toFile())) {
      ZipEntry entry = zip.getEntry("bundle.pb");
      if (entry == null || entry.isDirectory()) {
        throw new IOException("connector archive is missing bundle.pb");
      }
      byte[] bytes;
      try (InputStream in = zip.getInputStream(entry)) {
        bytes = readBounded(in, MAX_BUNDLE_BYTES);
      }
      try {
        var bundle = ConnectorTransferBundle.parseFrom(bytes);
        if (bundle.getFormatVersion() != FORMAT_VERSION) {
          throw new IOException(
              "unsupported connector archive format version: " + bundle.getFormatVersion());
        }
        return bundle;
      } catch (InvalidProtocolBufferException e) {
        throw new IOException("invalid connector archive bundle", e);
      }
    }
  }

  private static byte[] readBounded(InputStream in, int maxBytes) throws IOException {
    var output = new ByteArrayOutputStream(Math.min(maxBytes, 8192));
    byte[] buffer = new byte[8192];
    int total = 0;
    for (int read; (read = in.read(buffer)) != -1; ) {
      total += read;
      if (total > maxBytes) throw new IOException("connector archive bundle exceeds size limit");
      output.write(buffer, 0, read);
    }
    return output.toByteArray();
  }

  private static void put(ZipOutputStream zip, String name, byte[] bytes) throws IOException {
    var entry = new ZipEntry(name);
    entry.setTime(0L);
    zip.putNextEntry(entry);
    zip.write(bytes);
    zip.closeEntry();
  }

  private static String protobufJson(com.google.protobuf.MessageOrBuilder message)
      throws IOException {
    try {
      return JsonFormat.printer().includingDefaultValueFields().print(message) + "\n";
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("failed to render connector protobuf as JSON", e);
    }
  }

  private static String manifest(ConnectorTransferBundle bundle) {
    var out = new StringBuilder();
    out.append("{\n  \"formatVersion\": ").append(bundle.getFormatVersion());
    out.append(",\n  \"sourceAccountId\": \"")
        .append(jsonEscape(bundle.getSourceAccountId()))
        .append("\",");
    out.append("\n  \"exportedAt\": \"")
        .append(bundle.getExportedAt().getSeconds())
        .append(".")
        .append(bundle.getExportedAt().getNanos())
        .append("Z\",");
    out.append("\n  \"connectors\": [");
    for (int i = 0; i < bundle.getEntriesCount(); i++) {
      var entry = bundle.getEntries(i);
      if (i > 0) out.append(',');
      out.append("\n    {\"id\": \"")
          .append(jsonEscape(entry.getConnector().getResourceId().getId()))
          .append("\", \"displayName\": \"")
          .append(jsonEscape(entry.getConnector().getDisplayName()))
          .append("\", \"hasCredentials\": ")
          .append(entry.hasCredentials())
          .append('}');
    }
    if (bundle.getEntriesCount() > 0) out.append('\n').append("  ");
    return out.append("]\n}\n").toString();
  }

  private static String safeSegment(String value) {
    String safe = value == null ? "" : value.replaceAll("[^A-Za-z0-9._-]", "_");
    return safe.isBlank() ? "connector" : safe;
  }

  private static String jsonEscape(String value) {
    if (value == null) return "";
    var out = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '\\' -> out.append("\\\\");
        case '"' -> out.append("\\\"");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
          else out.append(c);
        }
      }
    }
    return out.toString();
  }

  private static void setOwnerOnly(Path path) {
    try {
      Set<PosixFilePermission> permissions =
          EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
      Files.setPosixFilePermissions(path, permissions);
    } catch (IOException | UnsupportedOperationException ignored) {
      // Non-POSIX filesystems do not expose these permissions.
    }
  }
}
