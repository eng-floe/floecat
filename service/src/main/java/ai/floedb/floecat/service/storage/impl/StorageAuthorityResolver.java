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

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

@ApplicationScoped
public class StorageAuthorityResolver {
  static final String STORAGE_AUTHORITY_SECRET_TYPE = "storage-authorities";

  @Inject SecretsManager secretsManager;

  ResolveStorageAuthorityResponse buildResponse(
      StorageAuthority authority,
      String locationPrefix,
      String accountId,
      boolean includeCredentials,
      boolean required,
      boolean serverSide) {
    if (authority == null) {
      if (required) {
        throw new IllegalArgumentException(
            "Credential vending was requested but no storage credential authority is configured for this table");
      }
      return ResolveStorageAuthorityResponse.getDefaultInstance();
    }
    ResolveStorageAuthorityResponse.Builder response =
        ResolveStorageAuthorityResponse.newBuilder().setAuthorityId(authority.getResourceId());
    response.putAllClientSafeConfig(clientSafeConfig(authority));
    if (!includeCredentials) {
      return response.build();
    }
    AuthCredentials authoritySecret =
        resolveAuthoritySecret(accountId, authority.getResourceId().getId())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Credential vending was requested but no storage credential authority is configured for this table"));
    ResolvedStorageCredentials resolved =
        serverSide
            ? resolveServerSideCredentials(authority, authoritySecret, locationPrefix)
            : mintTemporaryCredentials(authority, authoritySecret, locationPrefix);
    if (!serverSide && !resolved.isTemporary()) {
      throw new IllegalArgumentException(
          "Credential vending requires temporary storage credentials");
    }
    LinkedHashMap<String, String> storageConfig = new LinkedHashMap<>();
    storageConfig.put("type", authority.getType().isBlank() ? "s3" : authority.getType());
    storageConfig.putAll(clientSafeConfig(authority));
    storageConfig.putAll(resolved.asS3Properties());
    VendedStorageCredential.Builder credential =
        VendedStorageCredential.newBuilder()
            .setPrefix(locationPrefix)
            .putAllConfig(Map.copyOf(storageConfig));
    if (resolved.expiresAt() != null) {
      credential.setExpiresAt(
          com.google.protobuf.util.Timestamps.fromMillis(resolved.expiresAt().toEpochMilli()));
    }
    response.addStorageCredentials(credential.build());
    return response.build();
  }

  Optional<AuthCredentials> resolveAuthoritySecret(String accountId, String authorityId) {
    if (accountId == null || accountId.isBlank() || authorityId == null || authorityId.isBlank()) {
      return Optional.empty();
    }
    return secretsManager
        .get(accountId, STORAGE_AUTHORITY_SECRET_TYPE, authorityId)
        .map(
            payload -> {
              try {
                return AuthCredentials.parseFrom(payload);
              } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to parse storage credential authority secret", e);
              }
            });
  }

  ResolvedStorageCredentials mintTemporaryCredentials(
      StorageAuthority authority, AuthCredentials authoritySecret, String locationPrefix) {
    Optional<ResolvedStorageCredentials> resolved =
        CredentialResolverSupport.resolveStorageCredentials(authoritySecret);
    if (authoritySecret != null
        && authoritySecret.getCredentialCase() == AuthCredentials.CredentialCase.AWS
        && authority.hasAssumeRoleArn()
        && !authority.getAssumeRoleArn().isBlank()) {
      return assumeRoleFromStaticSource(authority, authoritySecret.getAws(), locationPrefix);
    }
    if (resolved.isPresent() && resolved.get().isTemporary()) {
      throw new IllegalArgumentException(
          "Credential vending requires scoped temporary storage credentials minted from a storage authority role");
    }
    throw new IllegalArgumentException(
        "Credential vending requires scoped temporary storage credentials minted from a storage authority role");
  }

  ResolvedStorageCredentials resolveServerSideCredentials(
      StorageAuthority authority, AuthCredentials authoritySecret, String locationPrefix) {
    if (authoritySecret != null
        && authoritySecret.getCredentialCase() == AuthCredentials.CredentialCase.AWS
        && authority.hasAssumeRoleArn()
        && !authority.getAssumeRoleArn().isBlank()) {
      return assumeRoleFromStaticSource(authority, authoritySecret.getAws(), locationPrefix);
    }
    return CredentialResolverSupport.resolveStorageCredentials(authoritySecret)
        .orElseThrow(
            () -> new IllegalArgumentException("Unsupported storage credential authority"));
  }

  Map<String, String> clientSafeConfig(StorageAuthority authority) {
    LinkedHashMap<String, String> computed = new LinkedHashMap<>();
    if (authority == null) {
      return Map.of();
    }
    if (authority.hasRegion() && !authority.getRegion().isBlank()) {
      putRegionConfig(computed, authority.getRegion());
    }
    if (authority.hasEndpoint() && !authority.getEndpoint().isBlank()) {
      computed.put("s3.endpoint", authority.getEndpoint());
    }
    if (authority.hasPathStyleAccess()) {
      computed.put("s3.path-style-access", Boolean.toString(authority.getPathStyleAccess()));
    }
    return computed.isEmpty() ? Map.of() : Map.copyOf(computed);
  }

  private ResolvedStorageCredentials assumeRoleFromStaticSource(
      StorageAuthority authority, AuthCredentials.AwsCredentials source, String locationPrefix) {
    AwsCredentialsProvider provider =
        source.getSessionToken() == null || source.getSessionToken().isBlank()
            ? StaticCredentialsProvider.create(
                AwsBasicCredentials.create(source.getAccessKeyId(), source.getSecretAccessKey()))
            : StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    source.getAccessKeyId(),
                    source.getSecretAccessKey(),
                    source.getSessionToken()));

    var builder = StsClient.builder().credentialsProvider(provider);
    if (authority.hasRegion() && !authority.getRegion().isBlank()) {
      builder.region(Region.of(authority.getRegion()));
    }

    Integer duration = authority.hasDurationSeconds() ? authority.getDurationSeconds() : null;
    AssumeRoleRequest request =
        AssumeRoleRequest.builder()
            .roleArn(authority.getAssumeRoleArn())
            .roleSessionName(
                firstNonBlank(
                    authority.hasAssumeRoleSessionName()
                        ? authority.getAssumeRoleSessionName()
                        : null,
                    "floecat-storage-authority"))
            .externalId(
                authority.hasAssumeRoleExternalId() ? authority.getAssumeRoleExternalId() : null)
            .policy(scopedSessionPolicy(locationPrefix))
            .durationSeconds(duration != null && duration > 0 ? duration : null)
            .build();

    try (StsClient sts = builder.build()) {
      Credentials credentials = sts.assumeRole(request).credentials();
      return new ResolvedStorageCredentials(
          credentials.accessKeyId(),
          credentials.secretAccessKey(),
          credentials.sessionToken(),
          credentials.expiration());
    }
  }

  private static String scopedSessionPolicy(String locationPrefix) {
    S3Location scope = S3Location.parse(locationPrefix);
    if (scope == null) {
      return null;
    }
    String bucket = jsonEscape(scope.bucket());
    String objectArn = jsonEscape(scope.objectArn());
    String listPrefix = jsonEscape(scope.listPrefix());
    return """
        {
          "Version":"2012-10-17",
          "Statement":[
            {
              "Effect":"Allow",
              "Action":["s3:ListBucket","s3:GetBucketLocation"],
              "Resource":["arn:aws:s3:::%s"],
              "Condition":{"StringLike":{"s3:prefix":["%s","%s/*"]}}
            },
            {
              "Effect":"Allow",
              "Action":[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
              ],
              "Resource":["%s","%s/*"]
            }
          ]
        }
        """
        .formatted(bucket, listPrefix, listPrefix, objectArn, objectArn)
        .replace('\n', ' ')
        .replaceAll("\\s+", " ")
        .trim();
  }

  private static String jsonEscape(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private record S3Location(String bucket, String keyPrefix) {
    static S3Location parse(String locationPrefix) {
      if (locationPrefix == null || locationPrefix.isBlank()) {
        return null;
      }
      String trimmed = locationPrefix.trim();
      String lower = trimmed.toLowerCase(java.util.Locale.ROOT);
      String normalized;
      if (lower.startsWith("s3://")) {
        normalized = trimmed.substring(5);
      } else if (lower.startsWith("s3a://") || lower.startsWith("s3n://")) {
        normalized = trimmed.substring(trimmed.indexOf("://") + 3);
      } else {
        return null;
      }
      int slash = normalized.indexOf('/');
      String bucket = slash < 0 ? normalized : normalized.substring(0, slash);
      if (bucket.isBlank()) {
        return null;
      }
      String prefix = slash < 0 ? "" : normalized.substring(slash + 1);
      while (prefix.endsWith("/")) {
        prefix = prefix.substring(0, prefix.length() - 1);
      }
      return new S3Location(bucket, prefix);
    }

    String objectArn() {
      return keyPrefix.isBlank()
          ? "arn:aws:s3:::%s".formatted(bucket)
          : "arn:aws:s3:::%s/%s".formatted(bucket, keyPrefix);
    }

    String listPrefix() {
      return keyPrefix;
    }
  }

  static Optional<StorageAuthority> resolveBest(
      List<StorageAuthority> authorities, String locationPrefix) {
    StorageAuthority best = null;
    if (authorities == null
        || authorities.isEmpty()
        || locationPrefix == null
        || locationPrefix.isBlank()) {
      return Optional.empty();
    }
    for (StorageAuthority authority : authorities) {
      if (authority == null
          || !authority.getEnabled()
          || authority.getLocationPrefix() == null
          || authority.getLocationPrefix().isBlank()
          || !matchesLocationPrefix(locationPrefix, authority.getLocationPrefix())) {
        continue;
      }
      if (best == null
          || stripTrailingSlash(authority.getLocationPrefix()).length()
              > stripTrailingSlash(best.getLocationPrefix()).length()) {
        best = authority;
      }
    }
    return Optional.ofNullable(best);
  }

  static boolean matchesLocationPrefix(String location, String configuredPrefix) {
    if (!isNonBlank(location) || !isNonBlank(configuredPrefix)) {
      return false;
    }
    String normalizedLocation = location.trim();
    String normalizedPrefix = stripTrailingSlash(configuredPrefix.trim());
    if (normalizedPrefix.isEmpty() || !normalizedLocation.startsWith(normalizedPrefix)) {
      return false;
    }
    if (normalizedLocation.length() == normalizedPrefix.length()) {
      return true;
    }
    return normalizedLocation.charAt(normalizedPrefix.length()) == '/';
  }

  static String stripTrailingSlash(String value) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    int end = value.length();
    while (end > 0 && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }

  static boolean isNonBlank(String value) {
    return value != null && !value.isBlank();
  }

  static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (isNonBlank(value)) {
        return value.trim();
      }
    }
    return null;
  }

  private static void putRegionConfig(Map<String, String> target, String region) {
    if (!isNonBlank(region)) {
      return;
    }
    target.put("s3.region", region);
    target.put("region", region);
    target.put("client.region", region);
  }
}
