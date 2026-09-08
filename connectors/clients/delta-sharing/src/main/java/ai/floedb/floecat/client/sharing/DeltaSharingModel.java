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
package ai.floedb.floecat.client.sharing;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Normalized Delta Sharing protocol objects.
 *
 * <p>Normalized rather than wire-shaped: the transport decodes what the server sent and hands these
 * on, so nothing above this package has to know that lists paginate, that metadata arrives as
 * NDJSON rather than a JSON object, or which of three cloud credential shapes a server chose.
 */
public final class DeltaSharingModel {

  private DeltaSharingModel() {}

  /** How a recipient may read a table's data. */
  public enum AccessMode {
    /** The server returns presigned per-file URLs from the query endpoint. */
    URL,
    /** The server issues temporary credentials for reading the table location directly. */
    DIR,
    /**
     * A mode this client does not recognise.
     *
     * <p>Kept rather than dropped so a stated list stays distinguishable from an absent field.
     * Dropping an unknown value made {@code ["dir2"]} decode to the empty list, which is the same
     * value used for "the server said nothing" -- so a table that had stated its modes was refused
     * under the strict setting with a message saying it had stated none.
     */
    OTHER
  }

  /** A share, the outermost level of the sharing hierarchy. */
  public record Share(String name, Optional<String> id) {
    public Share {
      name = requireText(name, "name");
      id = id == null ? Optional.empty() : id;
    }
  }

  /** A schema within a share. */
  public record Schema(String share, String name) {
    public Schema {
      share = requireText(share, "share");
      name = requireText(name, "name");
    }
  }

  /**
   * A table within a schema.
   *
   * <p>{@code accessModes} is empty when the server did not send the field. That is left as it
   * arrived rather than resolved here: the protocol reads an absent field as url only, but a caller
   * may reasonably ask the server instead, and which of those applies is a policy the catalog
   * provider owns through {@code delta.sharing.strict-access-modes}. A helper answering the strict
   * reading here would have quietly reinstated it for every future caller.
   *
   * <p>{@code location} is present only for directory access. A url-only server does not disclose
   * where the table lives, by design.
   *
   * <p>{@code id} is unique within the share, not across the server; {@code shareId} is the part
   * the protocol makes server-unique. An identity built from {@code id} alone says nothing about
   * which share it came from, and two shares may legally carry the same table id.
   */
  public record Table(
      String share,
      String schema,
      String name,
      Optional<String> id,
      Optional<String> shareId,
      Optional<String> location,
      List<String> auxiliaryLocations,
      List<AccessMode> accessModes) {

    public Table {
      share = requireText(share, "share");
      schema = requireText(schema, "schema");
      name = requireText(name, "name");
      id = id == null ? Optional.empty() : id;
      shareId = shareId == null ? Optional.empty() : shareId;
      location = location == null ? Optional.empty() : location;
      auxiliaryLocations = auxiliaryLocations == null ? List.of() : List.copyOf(auxiliaryLocations);
      accessModes = accessModes == null ? List.of() : List.copyOf(accessModes);
    }

    /** Whether this listing reports storage locations beyond the table's root. */
    public boolean hasAuxiliaryLocations() {
      return !auxiliaryLocations.isEmpty();
    }

    /** The share, schema and table names joined as the protocol addresses them. */
    public String fullName() {
      return share + "." + schema + "." + name;
    }
  }

  /**
   * The Protocol action heading a metadata or query response.
   *
   * <p>{@code minReaderVersion} above 1 means the table uses features that require {@code
   * responseformat=delta}, and {@code readerFeatures} names them.
   */
  public record Protocol(int minReaderVersion, List<String> readerFeatures) {
    public Protocol {
      readerFeatures = readerFeatures == null ? List.of() : List.copyOf(readerFeatures);
    }
  }

  /** The Metadata action describing a table's schema and layout. */
  public record TableMetadata(
      Optional<String> id,
      Optional<String> name,
      String format,
      String schemaJson,
      List<String> partitionColumns,
      Map<String, String> configuration,
      Optional<Long> version,
      Optional<String> location,
      List<String> auxiliaryLocations,
      List<AccessMode> accessModes) {

    public TableMetadata {
      id = id == null ? Optional.empty() : id;
      name = name == null ? Optional.empty() : name;
      format = requireText(format, "format");
      schemaJson = requireText(schemaJson, "schemaJson");
      partitionColumns = partitionColumns == null ? List.of() : List.copyOf(partitionColumns);
      configuration = configuration == null ? Map.of() : Map.copyOf(configuration);
      version = version == null ? Optional.empty() : version;
      location = location == null ? Optional.empty() : location;
      auxiliaryLocations = auxiliaryLocations == null ? List.of() : List.copyOf(auxiliaryLocations);
      accessModes = accessModes == null ? List.of() : List.copyOf(accessModes);
    }

    /** Whether this table reports auxiliary storage locations beyond its root. */
    public boolean hasAuxiliaryLocations() {
      return !auxiliaryLocations.isEmpty();
    }
  }

  /** A metadata response: the Protocol action, the Metadata action, and the reported version. */
  public record TableDescription(
      Protocol protocol, TableMetadata metadata, Optional<Long> version) {
    public TableDescription {
      Objects.requireNonNull(protocol, "protocol");
      Objects.requireNonNull(metadata, "metadata");
      version = version == null ? Optional.empty() : version;
    }
  }

  /** Which cloud a temporary credential is for. */
  public enum CredentialCloud {
    AWS,
    AZURE,
    GCP
  }

  /**
   * Temporary credentials for reading a table location directly.
   *
   * <p>Only the AWS shape carries key material here. Azure and GCP are recognised so the transport
   * can report which cloud a server answered with, rather than the caller seeing an empty
   * credential and having to guess why.
   */
  public record TemporaryCredentials(
      CredentialCloud cloud,
      String location,
      Optional<String> accessKeyId,
      Optional<String> secretAccessKey,
      Optional<String> sessionToken,
      Optional<Instant> expiresAt) {

    public TemporaryCredentials {
      Objects.requireNonNull(cloud, "cloud");
      location = requireText(location, "location");
      accessKeyId = accessKeyId == null ? Optional.empty() : accessKeyId;
      secretAccessKey = secretAccessKey == null ? Optional.empty() : secretAccessKey;
      sessionToken = sessionToken == null ? Optional.empty() : sessionToken;
      expiresAt = expiresAt == null ? Optional.empty() : expiresAt;
    }

    /** Whether this carries a complete AWS session triad. */
    public boolean hasAwsSession() {
      return cloud == CredentialCloud.AWS
          && accessKeyId.isPresent()
          && secretAccessKey.isPresent()
          && sessionToken.isPresent();
    }

    /**
     * Never the key material.
     *
     * <p>These records reach logs through exception messages and diagnostic lines, so the default
     * record toString would put a live secret in both.
     */
    @Override
    public String toString() {
      return "TemporaryCredentials[cloud="
          + cloud
          + ", location="
          + location
          + ", expiresAt="
          + expiresAt.map(Instant::toString).orElse("<none>")
          + ", credentials=<redacted>]";
    }
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
