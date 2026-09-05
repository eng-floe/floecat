/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.StorageLocations;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Exception;

@FunctionalInterface
interface UnityStorageAccessValidator {

  /** Implicitly public static final: this interface is package-private, so the constant is too. */
  String DELTA_LOG_DIR = "_delta_log/";

  void validate(String tableLocation, VendedStorageCredentials credentials);

  static UnityStorageAccessValidator s3() {
    return UnityStorageAccessValidator::validateS3;
  }

  private static void validateS3(String tableLocation, VendedStorageCredentials credentials) {
    // Parsed once, and inside the guard. Doing it here unguarded put an IllegalArgumentException
    // ahead of the check meant to catch it: an object key holding a character URI rejects -- a
    // space is the common one -- threw before s3Location could answer, and UnityCatalogErrors
    // mapped it to "storage access validation configuration is invalid", which describes neither
    // the location nor anything an operator configured.
    URI location = s3Location(tableLocation);
    if (location == null) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Unity Catalog storage validation needs an addressable S3 location, and could not read"
              + " a bucket from this one");
    }
    String bucketName = bucketOf(location);
    Map<String, String> properties = credentials.properties();
    String accessKey = required(properties, "s3.access-key-id");
    String secretKey = required(properties, "s3.secret-access-key");
    String sessionToken = nonBlank(properties.get("s3.session-token"));
    AwsCredentials awsCredentials =
        sessionToken == null
            ? AwsBasicCredentials.create(accessKey, secretKey)
            : AwsSessionCredentials.create(accessKey, secretKey, sessionToken);

    S3ClientBuilder builder =
        S3Client.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
            .region(Region.of(properties.getOrDefault("s3.region", "us-east-1")))
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(
                        Boolean.parseBoolean(properties.get("s3.path-style-access")))
                    .build())
            .overrideConfiguration(o -> o.addExecutionInterceptor(new BoundedListingBody()));
    String endpoint = nonBlank(properties.get("s3.endpoint"));
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }

    // The bucket named in the object URI, never s3.access-point. SourceCatalogCredentialVendor
    // strips that key before any reader sees it -- noteIgnoredAccessPoint records why -- so probing
    // the access point would validate an endpoint no scan ever addresses, and a grant scoped to it
    // alone would report success against reads that all get 403.
    String bucket = bucketName;
    String logPrefix = deltaLogPrefix(location.getPath());
    try (S3Client s3 = builder.build()) {
      var listing =
          s3.listObjectsV2(
              ListObjectsV2Request.builder().bucket(bucket).prefix(logPrefix).maxKeys(1).build());
      if (listing.contents().isEmpty()) {
        throw new CatalogAccessException(
            CatalogAccessException.Code.UNSUPPORTED,
            "Unity Catalog table storage contains no Delta log object to validate");
      }
      try {
        // Streamed and aborted, not buffered. Range is a request hint: an S3-compatible endpoint
        // that ignores it can answer with a body of any size, and toBytes() would hold all of it in
        // the shared service's heap. s3.endpoint is tenant-supplied, so that is a bound the client
        // has to enforce for itself -- the same reason the catalog and OAuth transports cap their
        // bodies. One byte is the whole question here: it proves the object was reachable and the
        // credential authorized it.
        try (var body =
            s3.getObject(
                GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(listing.contents().getFirst().key())
                    .range("bytes=0-0")
                    .build(),
                ResponseTransformer.toInputStream())) {
          body.read();
          // Abort rather than drain: closing alone would read the rest of whatever was sent to
          // return the connection to the pool.
          body.abort();
        }
      } catch (S3Exception rangeFailure) {
        // 416 on a zero-length object, which is what a directory marker is. The listing is
        // lexicographic and unbounded by a delimiter, so a marker keyed exactly "<table>/
        // _delta_log/" -- the S3 console's "create folder", some S3A mkdirs paths -- sorts ahead of
        // 00000000000000000000.json and is the key picked here. Not a failure of what this method
        // asks: the request reached the bucket and the credential authorized the object, which is
        // the whole question. Anything else stays a failure.
        if (rangeFailure.statusCode() != 416) {
          throw rangeFailure;
        }
      }
    } catch (CatalogAccessException failure) {
      throw failure;
    } catch (S3Exception failure) {
      String errorCode =
          failure.awsErrorDetails() == null ? null : failure.awsErrorDetails().errorCode();
      throw new CatalogAccessException(
          storageFailureCode(errorCode, failure.statusCode()),
          "Unity Catalog storage validation failed",
          failure);
    } catch (java.io.IOException failure) {
      // Reading or releasing the probe body: a fact about reaching the store, like the transport
      // failures below it, not about the credential.
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE,
          "Unity Catalog storage validation could not read the probe object",
          failure);
    } catch (RuntimeException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE,
          "Unity Catalog storage validation failed",
          failure);
    }
  }

  private static String required(Map<String, String> properties, String key) {
    String value = nonBlank(properties.get(key));
    if (value == null) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog storage credentials omitted " + key);
    }
    return value;
  }

  /**
   * How much of a listing response this client will read before giving up on it. A {@code
   * maxKeys(1)} listing answers in a few hundred bytes; the cap only has to bound the pathological
   * case.
   */
  static final long MAX_LISTING_RESPONSE_BYTES = 64L * 1024L;

  /**
   * Bounds the bytes the SDK will parse from a listing response.
   *
   * <p>{@code maxKeys} is a request hint and {@code s3.endpoint} is tenant-supplied -- the same
   * premise the ranged read already streams and aborts for. The listing had no matching bound: the
   * synchronous client parses the whole XML body and materialises every {@code Contents} entry
   * before the first key is read, so an endpoint that ignores {@code max-keys} could answer with a
   * listing of any size and spend the shared service's heap during validation.
   *
   * <p>Scoped to the listing on purpose. The ranged read streams a body of unknown size and aborts
   * after one byte, so capping that stream would constrain a path whose bound already holds.
   */
  final class BoundedListingBody implements ExecutionInterceptor {

    @Override
    public Optional<InputStream> modifyHttpResponseContent(
        Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
      Optional<InputStream> body = context.responseBody();
      if (!"ListObjectsV2"
          .equals(executionAttributes.getAttribute(SdkExecutionAttribute.OPERATION_NAME))) {
        return body;
      }
      return body.map(stream -> limited(stream, MAX_LISTING_RESPONSE_BYTES));
    }
  }

  /**
   * A stream that refuses to yield more than {@code limit} bytes.
   *
   * <p>Throws rather than reporting end-of-stream. A truncated XML body would otherwise parse as a
   * short listing, which reads as "this table has no Delta log object" -- a statement about the
   * table -- rather than as a response this client declined to read.
   */
  static InputStream limited(InputStream source, long limit) {
    return new FilterInputStream(source) {
      private long seen;

      private void count(int n) throws IOException {
        if (n <= 0) {
          return;
        }
        seen += n;
        if (seen > limit) {
          throw new IOException(
              "S3 listing response exceeded " + limit + " bytes; endpoint ignored max-keys");
        }
      }

      @Override
      public int read() throws IOException {
        int value = super.read();
        count(value < 0 ? 0 : 1);
        return value;
      }

      @Override
      public int read(byte[] buffer, int offset, int length) throws IOException {
        int n = super.read(buffer, offset, length);
        count(n);
        return n;
      }
    };
  }

  /**
   * The S3 key prefix the Delta log sits under, for a table location's path.
   *
   * <p>Package-private and pure so the cases can be asserted without S3. A table at the bucket root
   * has an empty path, and a leading slash there would produce "/_delta_log/" -- a key prefix that
   * matches nothing, so the log would read as absent even when it is present.
   */
  static String deltaLogPrefix(String locationPath) {
    String tablePrefix = stripLeadingSlash(locationPath);
    return tablePrefix.isEmpty()
        ? DELTA_LOG_DIR
        : tablePrefix + (tablePrefix.endsWith("/") ? "" : "/") + DELTA_LOG_DIR;
  }

  /**
   * How an S3 failure during validation maps onto a catalog-access code.
   *
   * <p>Package-private and pure for the same reason. The distinction that matters is between a
   * credential that has expired, one that is refused, and a bucket that could not be reached:
   * validation reports the first two as facts about the credential and the third as a fact about
   * the upstream.
   */
  static CatalogAccessException.Code storageFailureCode(String errorCode, int statusCode) {
    if ("ExpiredToken".equals(errorCode)) {
      return CatalogAccessException.Code.CREDENTIAL_EXPIRED;
    }
    if (statusCode == 401 || statusCode == 403) {
      return CatalogAccessException.Code.PERMISSION_DENIED;
    }
    // A wrong region is configuration, not an unreachable bucket, and it is the first thing a new
    // deployment hits: s3.region defaults to us-east-1 on both sides, so an operator with a bucket
    // elsewhere who sets neither property lands here. S3 answers a redirect for the bucket and a
    // malformed-authorization for the signing region; reporting either as "unavailable" sends them
    // looking at the network instead of at the property.
    if ("PermanentRedirect".equals(errorCode)
        || "AuthorizationHeaderMalformed".equals(errorCode)
        || statusCode == 301) {
      return CatalogAccessException.Code.INVALID_CONFIGURATION;
    }
    return CatalogAccessException.Code.UNAVAILABLE;
  }

  /**
   * The location parsed as an addressable S3 URI, or {@code null} when it is not one.
   *
   * <p>Pure, and the whole decision in one place so it can be asserted without an S3 client -- and
   * so the parse cannot run ahead of the guard that reports it. {@code normalizeScheme} rewrites a
   * whole location rather than a bare scheme, and folds s3a and s3n into s3, which {@code
   * VendedStorageCredentials.covers} relies on: an external location registered as {@code
   * s3a://bucket/db/tbl} vends and has to validate too. Comparing the scheme literally failed such
   * a table as "not an S3 location", and the storage check has no per-table skip, so one of them
   * reported the whole integration invalid.
   *
   * <p>A key holding a character {@link URI} rejects -- a space, most often -- reads as {@code
   * null} here rather than throwing. That refuses the table rather than addressing it, which is a
   * real limit: such keys are legal in S3, and reaching them means parsing bucket and key without
   * {@code URI} at all, since {@code getPath} also decodes percent-escapes the store treats
   * literally.
   */
  static URI s3Location(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    URI normalized;
    try {
      normalized = URI.create(StorageLocations.normalizeScheme(tableLocation));
    } catch (IllegalArgumentException notAUri) {
      return null;
    }
    if (!"s3".equalsIgnoreCase(normalized.getScheme()) || bucketOf(normalized) == null) {
      return null;
    }
    return normalized;
  }

  /** The bucket {@link #s3Location} would address, or {@code null}; kept for direct assertion. */
  static String s3Bucket(String tableLocation) {
    URI location = s3Location(tableLocation);
    return location == null ? null : bucketOf(location);
  }

  /**
   * The bucket a location names, or {@code null} when it names none.
   *
   * <p>{@code getHost} applies RFC reg-name rules, so a bucket whose name contains an underscore
   * parses with a null host -- legal for buckets created in us-east-1 before the 2018 naming rules,
   * and still in use. Reporting one as "not an S3 location" points an operator at their storage
   * backend rather than at a parser rule, and storage-access failure fails the whole Integration.
   *
   * <p>The authority is the fallback, refused if it carries userinfo or a port: an S3 bucket has
   * neither, so anything that does is not a bucket name this should hand to the SDK.
   */
  static String bucketOf(URI location) {
    String host = nonBlank(location.getHost());
    if (host != null) {
      return host;
    }
    String authority = nonBlank(location.getAuthority());
    if (authority == null || authority.indexOf('@') >= 0 || authority.indexOf(':') >= 0) {
      return null;
    }
    return authority;
  }

  private static String stripLeadingSlash(String value) {
    return value == null ? "" : value.replaceFirst("^/+", "");
  }

  private static String nonBlank(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }
}
