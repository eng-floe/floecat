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
package ai.floedb.floecat.http.guards;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Objects;

/**
 * Transport gates for an operator-supplied catalog endpoint, shared by every client that accepts
 * one.
 *
 * <p>Deny by default. HTTPS is required; cleartext to a loopback host and a base URI naming a
 * private address literal each need an explicit opt-in; link-local, wildcard and multicast literals
 * are always refused, the cloud metadata address among them.
 *
 * <p>The checks are deliberately literal-only. Deciding them for a hostname means resolving it, and
 * a resolver here disagrees with the resolution {@code HttpClient} performs at connect time. A
 * hostname pointing at a private address is therefore network policy rather than something this
 * class claims to prevent.
 *
 * <p>Extracted from {@code HttpUnityCatalogClient}, which held the only copy. The subtleties are
 * why this is shared rather than reimplemented per client: a zone-scoped literal parses as a
 * hostname and would skip the address-class gate, an all-numeric host is resolved modulo 2^32 by
 * the transport so {@code 7147006462} reaches the metadata address, and {@code localhost} matches
 * exactly rather than as a suffix because a tenant-controlled zone can point {@code x.localhost} at
 * a public address that would then receive an Authorization header in cleartext.
 */
public final class HttpEndpointGuards {

  /** Property permitting cleartext HTTP to a loopback host. */
  public static final String ALLOW_LOOPBACK_PROPERTY =
      "floecat.security.allow-loopback-catalog-endpoints";

  private static final String ALLOW_LOOPBACK_ENV =
      "FLOECAT_SECURITY_ALLOW_LOOPBACK_CATALOG_ENDPOINTS";

  /** Property permitting a base URI that names a private address literal. */
  public static final String ALLOW_PRIVATE_PROPERTY =
      "floecat.security.allow-private-catalog-endpoints";

  private static final String ALLOW_PRIVATE_ENV =
      "FLOECAT_SECURITY_ALLOW_PRIVATE_CATALOG_ENDPOINTS";

  private HttpEndpointGuards() {}

  public static URI requireAllowedEndpoint(URI baseUri, String subject) {
    Objects.requireNonNull(baseUri, "baseUri");
    // Before any check whose message interpolates the URI. Credentials belong in
    // UnityCatalogAuthentication, and the JDK client does not transmit them.
    if (baseUri.getUserInfo() != null
        || baseUri.getRawUserInfo() != null
        || authorityCarriesUserInfo(baseUri)) {
      throw new IllegalArgumentException(
          subject + " must not contain userinfo; supply credentials out of band");
    }
    if (!baseUri.isAbsolute()) {
      throw new IllegalArgumentException(subject + " must be absolute: " + display(baseUri));
    }
    String scheme = baseUri.getScheme();
    boolean https = "https".equalsIgnoreCase(scheme);
    boolean loopbackHttp =
        "http".equalsIgnoreCase(scheme)
            && allowLoopbackCleartext()
            && isLoopbackHost(baseUri.getHost());
    if (!https && !loopbackHttp) {
      throw new IllegalArgumentException(
          subject
              + " must use HTTPS, except HTTP is allowed for loopback hosts when "
              + ALLOW_LOOPBACK_PROPERTY
              + " is set: "
              + display(baseUri));
    }
    if (baseUri.getHost() == null) {
      throw new IllegalArgumentException(subject + " must include a host: " + display(baseUri));
    }
    if (baseUri.getRawQuery() != null || baseUri.getRawFragment() != null) {
      throw new IllegalArgumentException(
          subject + " must not include a query or fragment: " + display(baseUri));
    }
    // -1 is absent. URI and HttpRequest.Builder both accept 65536; InetSocketAddress rejects it at
    // send time, where it is reported as TRANSPORT.
    int port = baseUri.getPort();
    if (port != -1 && (port < 1 || port > 65535)) {
      throw new IllegalArgumentException(
          subject + " port must be between 1 and 65535: " + display(baseUri));
    }
    assertAddressClassAllowed(baseUri, subject);
    return baseUri;
  }

  /**
   * Rejects address classes a tenant-supplied connector URI must not name.
   *
   * <p>Link-local, wildcard, multicast, broadcast and {@code 0.0.0.0/8} literals are refused
   * outright; no catalog is reachable at one, and {@code https://169.254.169.254} is otherwise a
   * well-formed HTTPS URI for a cloud metadata service. Site-local literals require {@link
   * #ALLOW_PRIVATE_PROPERTY}, since an internal catalog is an ordinary deployment. Loopback is
   * allowed; cleartext to it is governed by {@link #ALLOW_LOOPBACK_PROPERTY}.
   *
   * <p>Literals only: resolving a hostname here would disagree with the resolution {@code
   * HttpClient} performs at connect time, so a hostname is out of scope.
   */
  private static void assertAddressClassAllowed(URI baseUri, String subject) {
    String host = unbracket(baseUri.getHost());
    // A zone id names a local interface, and ofLiteral cannot parse one. Left to the catch below a
    // scoped literal reads as a hostname and skips this gate entirely, so fe80::1%eth0 admits a
    // link-local address. The transport cannot carry one either.
    if (host.indexOf('%') >= 0) {
      throw new IllegalArgumentException(
          subject + " must not name a zone-scoped address: " + display(baseUri));
    }
    InetAddress address;
    try {
      address = InetAddress.ofLiteral(host);
    } catch (IllegalArgumentException notALiteral) {
      // A host of only digits and dots is not a hostname -- a DNS name cannot be entirely numeric
      // -- and the resolver the transport uses accepts these modulo 2^32 even though ofLiteral
      // refuses them: 7147006462 resolves to 169.254.169.254 and 4294967296 to 0.0.0.0. Returning
      // here would hand the transport a link-local or wildcard target this gate refuses by name.
      if (isNumericHost(host)) {
        throw new IllegalArgumentException(
            subject
                + " must not name a numeric host that is not an address literal: "
                + display(baseUri));
      }
      return;
    }
    if (address.isLoopbackAddress()) {
      return;
    }
    if (address.isLinkLocalAddress()
        || address.isAnyLocalAddress()
        || address.isMulticastAddress()
        || isBroadcastOrThisNetwork(address)) {
      throw new IllegalArgumentException(
          subject
              + " must not name a link-local, wildcard or multicast address: "
              + display(baseUri));
    }
    if (isSiteLocal(address) && !allowPrivateAddresses()) {
      throw new IllegalArgumentException(
          subject
              + " names a private address, which requires "
              + ALLOW_PRIVATE_PROPERTY
              + ": "
              + display(baseUri));
    }
  }

  private static boolean isLoopbackHost(String host) {
    if (host == null) {
      return false;
    }
    String normalized = host.toLowerCase(Locale.ROOT);
    if (normalized.startsWith("[") && normalized.endsWith("]")) {
      normalized = normalized.substring(1, normalized.length() - 1);
    }
    if (normalized.endsWith(".")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    // Exactly "localhost", not any *.localhost name. RFC 6761 says such names should resolve to
    // loopback, but nothing here enforces that and this gate does not resolve: a zone the tenant
    // controls can point catalog.localhost at a public address, and the Authorization header would
    // then go out in cleartext to it. CredentialResolverSupport, which this mirrors, has no suffix
    // rule either -- it resolves and requires every answer to be loopback.
    if (normalized.equals("localhost")) {
      return true;
    }
    // ofLiteral, never getByName: a host this cannot decide is denied, not resolved. A resolver
    // here disagrees with the one HttpClient uses at connect time. Character-shape pre-filters are
    // no substitute: "4294967296" and "1." are all digits and dots yet parse as no address.
    try {
      return InetAddress.ofLiteral(normalized).isLoopbackAddress();
    } catch (IllegalArgumentException notAnAddressLiteral) {
      return false;
    }
  }

  /**
   * Whether the authority embeds userinfo that {@link URI#getUserInfo()} does not report. {@code
   * java.net.URI} populates it only for a server-based authority; a host it rejects as a hostname
   * -- an underscore, a non-numeric port -- yields a registry-based authority with the credential
   * still in the raw string. {@code @} delimits userinfo and has no other role there.
   */
  private static boolean authorityCarriesUserInfo(URI baseUri) {
    String authority = baseUri.getRawAuthority();
    return authority != null && authority.indexOf('@') >= 0;
  }

  /**
   * The base URI as it may appear in a rejection message: scheme, authority and path only.
   *
   * <p>A query is a common place for a token, and the userinfo guard covers only credentials in the
   * authority. Every gate here reports the value it rejected, so the display form drops the two
   * components that carry data rather than address the endpoint.
   */
  private static String display(URI baseUri) {
    if (baseUri == null) {
      return "null";
    }
    StringBuilder shown = new StringBuilder();
    if (baseUri.getScheme() != null) {
      shown.append(baseUri.getScheme()).append("://");
    }
    // Parsed components only, never the raw authority. URI reports a host it cannot parse as
    // server-based -- an underscore, a bad port, or a percent-encoded delimiter such as
    // alice:s3cr3t%40host -- by leaving getHost() and both userinfo accessors null while the raw
    // authority keeps the credential. Echoing that here would undo the userinfo guard for exactly
    // the inputs it cannot recognise.
    if (baseUri.getHost() == null) {
      shown.append("<unparseable-authority>");
    } else {
      shown.append(baseUri.getHost());
      if (baseUri.getPort() != -1) {
        shown.append(':').append(baseUri.getPort());
      }
    }
    if (baseUri.getRawPath() != null) {
      shown.append(baseUri.getRawPath());
    }
    return shown.isEmpty() ? "<empty>" : shown.toString();
  }

  private static String unbracket(String host) {
    String value = host == null ? "" : host.trim();
    if (value.startsWith("[") && value.endsWith("]")) {
      value = value.substring(1, value.length() - 1);
    }
    return value.endsWith(".") ? value.substring(0, value.length() - 1) : value;
  }

  /** Whether every character is an ASCII digit or a dot. Deliberately not {@code isDigit}. */
  private static boolean isNumericHost(String host) {
    if (host.isEmpty()) {
      return false;
    }
    for (int i = 0; i < host.length(); i++) {
      char c = host.charAt(i);
      if ((c < '0' || c > '9') && c != '.') {
        return false;
      }
    }
    return true;
  }

  /** IPv4 RFC 1918 and IPv6 unique-local, which {@code isSiteLocalAddress} misses for fc00::/7. */
  private static boolean isSiteLocal(InetAddress address) {
    if (address.isSiteLocalAddress()) {
      return true;
    }
    byte[] bytes = address.getAddress();
    return bytes.length == 16 && (bytes[0] & 0xFE) == 0xFC;
  }

  /**
   * IPv4 limited broadcast and {@code 0.0.0.0/8}. {@code isMulticastAddress} covers 224/4 and
   * {@code isAnyLocalAddress} only the exact wildcard, so neither address is caught by them.
   */
  private static boolean isBroadcastOrThisNetwork(InetAddress address) {
    byte[] bytes = address.getAddress();
    if (bytes.length != 4) {
      return false;
    }
    boolean broadcast = true;
    for (byte octet : bytes) {
      broadcast &= octet == (byte) 0xFF;
    }
    return broadcast || bytes[0] == 0;
  }

  private static boolean allowPrivateAddresses() {
    return Boolean.parseBoolean(
        System.getProperty(
            ALLOW_PRIVATE_PROPERTY, System.getenv().getOrDefault(ALLOW_PRIVATE_ENV, "false")));
  }

  private static boolean allowLoopbackCleartext() {
    return Boolean.parseBoolean(
        System.getProperty(
            ALLOW_LOOPBACK_PROPERTY, System.getenv().getOrDefault(ALLOW_LOOPBACK_ENV, "false")));
  }

  /**
   * The address-class policy alone, for a second tenant-supplied endpoint in a client's care.
   *
   * <p>A client that derives a token endpoint from a catalog URI POSTs credentials to a URI the
   * tenant chose, which is the one request in such a path carrying a secret. It has to answer this
   * question the same way the catalog endpoint does.
   */
  public static void assertEndpointAddressAllowed(URI endpoint) {
    assertAddressClassAllowed(Objects.requireNonNull(endpoint, "endpoint"), "Catalog endpoint");
  }

  /**
   * HTTPS unless a deployment says otherwise: {@value #ALLOW_CLEARTEXT_S3_PROPERTY}, or the
   * environment variable {@value #ALLOW_CLEARTEXT_S3_ENV}.
   */
  public static final String ALLOW_CLEARTEXT_S3_PROPERTY =
      "floecat.security.allow-cleartext-s3-endpoints";

  public static final String ALLOW_CLEARTEXT_S3_ENV =
      "FLOECAT_SECURITY_ALLOW_CLEARTEXT_S3_ENDPOINTS";

  /**
   * Rejects a storage endpoint that names somewhere the service should not be made to reach.
   *
   * <p>Checked when a client is opened, before anything connects. The value is tenant-supplied and
   * reaches {@code endpointOverride}, so validation would otherwise issue signed S3 requests
   * wherever it pointed, and the same value travels on as client-safe routing to reconcile and
   * query workers.
   *
   * <p>HTTPS unless a deployment says otherwise. An S3 request carries a SigV4 signature rather
   * than a bearer secret, which would make cleartext defensible -- except that a provider vending
   * storage credentials publishes a session token, every signed request then carries it in {@code
   * X-Amz-Security-Token}, and a session token is replayable against the table's prefix by anyone
   * who sees it for as long as it lives.
   *
   * <p>The escape hatch stays, because an S3-compatible endpoint on a private network commonly is
   * HTTP -- MinIO and LocalStack both -- but it is a deployment saying so rather than a default.
   * The address-class rule still applies on top of either scheme: it is the one that refuses {@code
   * 169.254.169.254}.
   *
   * @param subject how the endpoint is named in a refusal, such as {@code "Unity Catalog
   *     s3.endpoint"}
   * @throws IllegalArgumentException naming what was refused, for a caller to wrap in whatever its
   *     own contract raises
   */
  public static void requireUsableStorageEndpoint(String endpoint, String subject) {
    Objects.requireNonNull(subject, "subject");
    if (endpoint == null || endpoint.isBlank()) {
      return;
    }
    URI uri;
    try {
      uri = URI.create(endpoint);
    } catch (IllegalArgumentException malformed) {
      // The cause is never attached, and neither is the value. URI.create quotes the whole input in
      // its message and again in the nested URISyntaxException, so an endpoint carrying userinfo or
      // a signed query -- which is what the checks below exist to refuse -- put that secret into
      // the chain a validation failure logs, on the one path where the value was never parseable
      // enough to be redacted. A parse position says where without saying what.
      throw new IllegalArgumentException(
          subject + " is not a valid URI: " + parsePosition(malformed));
    }
    String scheme = uri.getScheme();
    if (!uri.isAbsolute()
        || uri.getHost() == null
        || !("https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme))
        || uri.getRawUserInfo() != null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null) {
      throw new IllegalArgumentException(
          subject + " must be an absolute http or https URI with no userinfo, query or fragment");
    }
    if ("http".equalsIgnoreCase(scheme) && !allowCleartextS3Endpoints()) {
      throw new IllegalArgumentException(
          subject
              + " must use HTTPS: a vended credential carries a session token, which travels in a"
              + " header and is replayable. Set "
              + ALLOW_CLEARTEXT_S3_ENV
              + "=true to allow cleartext on a trusted network");
    }
    assertAddressClassAllowed(uri, subject);
  }

  /**
   * Where a parse failed, never what failed to parse.
   *
   * <p>{@code URI.create} reports "Malformed escape pair at index 34: <the whole input>". The index
   * is the diagnostic; the input is the secret.
   */
  private static String parsePosition(IllegalArgumentException malformed) {
    Throwable cause = malformed.getCause();
    String message =
        cause instanceof URISyntaxException syntax
            ? syntax.getReason() + " at index " + syntax.getIndex()
            : "could not be parsed";
    return message;
  }

  private static boolean allowCleartextS3Endpoints() {
    return Boolean.parseBoolean(
        System.getProperty(
            ALLOW_CLEARTEXT_S3_PROPERTY,
            System.getenv().getOrDefault(ALLOW_CLEARTEXT_S3_ENV, "false")));
  }

  /**
   * Whether cleartext is permitted to this endpoint because it is loopback and the opt-in is set.
   *
   * <p>Exposed so a derived token endpoint can answer it the same way. An integration against an
   * HTTP loopback catalog, the ordinary local-dev shape, derives its token endpoint from the
   * catalog URI and inherits the {@code http} scheme; holding that endpoint to HTTPS with no escape
   * hatch makes client-credentials authentication impossible to run locally while the catalog
   * request beside it is allowed.
   */
  public static boolean isCleartextLoopbackAllowed(URI endpoint) {
    Objects.requireNonNull(endpoint, "endpoint");
    return allowLoopbackCleartext() && isLoopbackHost(endpoint.getHost());
  }
}
