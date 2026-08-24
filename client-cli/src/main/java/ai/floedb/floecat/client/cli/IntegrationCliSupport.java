/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.client.cli.util.CliUtils;
import ai.floedb.floecat.client.cli.util.FQNameParserUtil;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.AwsAccessKeyAuthentication;
import ai.floedb.floecat.integration.rpc.AwsAccessKeySecret;
import ai.floedb.floecat.integration.rpc.AwsAssumeRoleAuthentication;
import ai.floedb.floecat.integration.rpc.AwsDefaultAuthentication;
import ai.floedb.floecat.integration.rpc.AwsSigV4Authentication;
import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationSpec;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationsGrpc;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.integration.rpc.CatalogOverlaySpec;
import ai.floedb.floecat.integration.rpc.CatalogOverlaysGrpc;
import ai.floedb.floecat.integration.rpc.CreateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.CreateCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.DeleteCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.GetCatalogOverlayRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogIntegrationsRequest;
import ai.floedb.floecat.integration.rpc.ListCatalogOverlaysRequest;
import ai.floedb.floecat.integration.rpc.NamespacePath;
import ai.floedb.floecat.integration.rpc.OAuthClientCredentialsAuthentication;
import ai.floedb.floecat.integration.rpc.SecretValue;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationAuthenticationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogIntegrationRequest;
import ai.floedb.floecat.integration.rpc.UpdateCatalogOverlayRequest;
import com.google.protobuf.FieldMask;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/** Small CRUD surface for catalog integrations and their catalog overlays. */
final class IntegrationCliSupport {
  private static final int DEFAULT_PAGE_SIZE = 100;

  private IntegrationCliSupport() {}

  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations,
      CatalogOverlaysGrpc.CatalogOverlaysBlockingStub overlays,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    switch (command) {
      case "integrations" -> listIntegrations(out, integrations);
      case "integration" -> integrationCrud(args, out, integrations, getCurrentAccountId);
      case "overlays" -> listOverlays(args, out, integrations, overlays, getCurrentAccountId);
      case "overlay" ->
          overlayCrud(args, out, integrations, overlays, directory, getCurrentAccountId);
      default -> throw new IllegalArgumentException("Unsupported integration command: " + command);
    }
  }

  private static void integrationCrud(
      List<String> args,
      PrintStream out,
      CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations,
      Supplier<String> accountId) {
    if (args.isEmpty()) {
      out.println("usage: integration <list|get|create|update|update-auth|delete> ...");
      return;
    }
    switch (args.getFirst()) {
      case "list" -> listIntegrations(out, integrations);
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: integration get <name|id>");
          return;
        }
        var row =
            integrations
                .getCatalogIntegration(integrationSelector(args.get(1), accountId))
                .getIntegration();
        printIntegrations(List.of(row), out);
      }
      case "create" -> {
        if (args.size() < 4) {
          out.println(
              "usage: integration create <name> <iceberg-rest|unity> <uri>"
                  + " --auth-type <type> [--auth k=v ...] [--cred k=v ...]"
                  + " [--props k=v ...]");
          return;
        }
        ParsedAuthentication parsedAuthentication = parseAuthentication(args);
        Map<String, String> properties = CliUtils.parseKeyValueList(args, "--props");
        CatalogIntegrationSpec spec =
            integrationSpec(
                Quotes.unquote(args.get(1)),
                parseIntegrationType(Quotes.unquote(args.get(2))),
                Quotes.unquote(args.get(3)),
                parsedAuthentication.authentication(),
                properties);
        var row =
            integrations
                .createCatalogIntegration(
                    CreateCatalogIntegrationRequest.newBuilder()
                        .setSpec(spec)
                        .setCredentials(parsedAuthentication.credentials())
                        .build())
                .getIntegration();
        printIntegrations(List.of(row), out);
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: integration update <name|id> [--display <name>] [--uri <uri>]"
                  + " [--props k=v ...] [--etag <etag>]");
          return;
        }
        ResourceId id = resolveIntegration(args.get(1), integrations, accountId);
        FieldMask updateMask = integrationUpdateMask(args);
        if (updateMask.getPathsCount() == 0) {
          throw new IllegalArgumentException("No integration changes specified");
        }
        var request =
            UpdateCatalogIntegrationRequest.newBuilder()
                .setIntegrationId(id)
                .setSpec(integrationUpdateSpec(args))
                .setUpdateMask(updateMask);
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) request.setPrecondition(precondition);
        printIntegrations(
            List.of(integrations.updateCatalogIntegration(request.build()).getIntegration()), out);
      }
      case "update-auth" -> {
        if (args.size() < 2) {
          out.println(
              "usage: integration update-auth <name|id> --auth-type <type>"
                  + " [--auth k=v ...] [--cred k=v ...] [--etag <etag>]");
          return;
        }
        ParsedAuthentication parsedAuthentication = parseAuthentication(args);
        var request =
            UpdateCatalogIntegrationAuthenticationRequest.newBuilder()
                .setIntegrationId(resolveIntegration(args.get(1), integrations, accountId))
                .setAuthentication(parsedAuthentication.authentication())
                .setCredentials(parsedAuthentication.credentials());
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) request.setPrecondition(precondition);
        printIntegrations(
            List.of(
                integrations
                    .updateCatalogIntegrationAuthentication(request.build())
                    .getIntegration()),
            out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: integration delete <name|id> [--cascade] [--etag <etag>]");
          return;
        }
        var request =
            DeleteCatalogIntegrationRequest.newBuilder()
                .setIntegrationId(resolveIntegration(args.get(1), integrations, accountId));
        request.setCascade(args.contains("--cascade"));
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) request.setPrecondition(precondition);
        integrations.deleteCatalogIntegration(request.build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private static void overlayCrud(
      List<String> args,
      PrintStream out,
      CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations,
      CatalogOverlaysGrpc.CatalogOverlaysBlockingStub overlays,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> accountId) {
    if (args.isEmpty()) {
      out.println("usage: overlay <list|get|create|update|delete> ...");
      return;
    }
    switch (args.getFirst()) {
      case "list" -> listOverlays(args, out, integrations, overlays, accountId);
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: overlay get <name|id>");
          return;
        }
        var row = overlays.getCatalogOverlay(overlaySelector(args.get(1), accountId)).getOverlay();
        printOverlays(List.of(row), out);
      }
      case "create" -> {
        if (args.size() < 4) {
          out.println(
              "usage: overlay create <name> <integration-name|id> <catalog-name|id>"
                  + " [--include a.b,c.d] [--exclude x.y]");
          return;
        }
        var spec =
            overlaySpec(
                Quotes.unquote(args.get(1)),
                resolveIntegration(args.get(2), integrations, accountId),
                CatalogCliSupport.resolveCatalogId(args.get(3), directory, accountId),
                args);
        var row =
            overlays
                .createCatalogOverlay(
                    CreateCatalogOverlayRequest.newBuilder().setSpec(spec).build())
                .getOverlay();
        printOverlays(List.of(row), out);
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: overlay update <name|id> [--display <name>] [--include a.b,c.d]"
                  + " [--exclude x.y]"
                  + " [--etag <etag>]");
          return;
        }
        ResourceId id = resolveOverlay(args.get(1), overlays, accountId);
        FieldMask updateMask = overlayUpdateMask(args);
        if (updateMask.getPathsCount() == 0) {
          throw new IllegalArgumentException("No overlay changes specified");
        }
        var request =
            UpdateCatalogOverlayRequest.newBuilder()
                .setOverlayId(id)
                .setSpec(overlayUpdateSpec(args))
                .setUpdateMask(updateMask);
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) request.setPrecondition(precondition);
        printOverlays(List.of(overlays.updateCatalogOverlay(request.build()).getOverlay()), out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: overlay delete <name|id> [--etag <etag>]");
          return;
        }
        var request =
            DeleteCatalogOverlayRequest.newBuilder()
                .setOverlayId(resolveOverlay(args.get(1), overlays, accountId));
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) request.setPrecondition(precondition);
        overlays.deleteCatalogOverlay(request.build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private static CatalogIntegrationSpec integrationSpec(
      String name,
      CatalogIntegrationType type,
      String uri,
      CatalogAuthentication authentication,
      Map<String, String> properties) {
    return CatalogIntegrationSpec.newBuilder()
        .setDisplayName(name)
        .setType(type)
        .setCatalogUri(uri)
        .setAuthentication(authentication)
        .putAllProperties(properties)
        .build();
  }

  private static ParsedAuthentication parseAuthentication(List<String> args) {
    String authType = normalized(CliArgs.parseStringFlag(args, "--auth-type", ""));
    if (authType.isBlank()) {
      throw new IllegalArgumentException("Missing --auth-type");
    }
    Map<String, String> auth = new LinkedHashMap<>(CliUtils.parseKeyValueList(args, "--auth"));
    Map<String, String> credentials =
        new LinkedHashMap<>(CliUtils.parseKeyValueList(args, "--cred"));
    var authentication = CatalogAuthentication.newBuilder();
    var secret = CatalogIntegrationCredentials.newBuilder();

    switch (authType) {
      case "OAUTH_CLIENT_CREDENTIALS", "OAUTH-CLIENT-CREDENTIALS", "OAUTH2" -> {
        var oauth =
            OAuthClientCredentialsAuthentication.newBuilder()
                .setClientId(require(auth, "client_id", "--auth"));
        setOptional(auth, "token_uri", oauth::setTokenUri);
        String scopes = take(auth, "scopes");
        if (scopes != null) oauth.addAllScopes(CliUtils.csvList(scopes));
        authentication.setOauthClientCredentials(oauth);
        secret.setOauthClientSecret(
            SecretValue.newBuilder().setValue(require(credentials, "client_secret", "--cred")));
      }
      case "BEARER" -> {
        authentication.setBearer(BearerAuthentication.getDefaultInstance());
        secret.setBearerToken(
            SecretValue.newBuilder().setValue(require(credentials, "token", "--cred")));
      }
      case "AWS_ASSUME_ROLE", "AWS-ASSUME-ROLE" ->
          authentication.setAwsAssumeRole(assumeRole(auth));
      case "AWS_ACCESS_KEY", "AWS-ACCESS-KEY" -> {
        authentication.setAwsAccessKey(
            AwsAccessKeyAuthentication.newBuilder()
                .setAccessKeyId(require(auth, "access_key_id", "--auth")));
        secret.setAwsAccessKey(accessKeySecret(credentials));
      }
      case "AWS_SIGV4", "AWS-SIGV4" -> {
        var sigv4 =
            AwsSigV4Authentication.newBuilder().setRegion(require(auth, "region", "--auth"));
        setOptional(auth, "signing_name", sigv4::setSigningName);
        String source = normalized(require(auth, "credential_source", "--auth"));
        switch (source) {
          case "DEFAULT", "AWS_DEFAULT", "AWS-DEFAULT" ->
              sigv4.setAwsDefault(AwsDefaultAuthentication.getDefaultInstance());
          case "ASSUME_ROLE", "ASSUME-ROLE", "AWS_ASSUME_ROLE", "AWS-ASSUME-ROLE" ->
              sigv4.setAwsAssumeRole(assumeRole(auth));
          case "ACCESS_KEY", "ACCESS-KEY", "AWS_ACCESS_KEY", "AWS-ACCESS-KEY" -> {
            sigv4.setAwsAccessKey(
                AwsAccessKeyAuthentication.newBuilder()
                    .setAccessKeyId(require(auth, "access_key_id", "--auth")));
            secret.setAwsAccessKey(accessKeySecret(credentials));
          }
          default ->
              throw new IllegalArgumentException("Unsupported --auth credential_source: " + source);
        }
        authentication.setAwsSigv4(sigv4);
      }
      default -> throw new IllegalArgumentException("Unsupported --auth-type: " + authType);
    }

    rejectUnknown(auth, "--auth");
    rejectUnknown(credentials, "--cred");
    return new ParsedAuthentication(authentication.build(), secret.build());
  }

  private static AwsAssumeRoleAuthentication.Builder assumeRole(Map<String, String> auth) {
    var assumeRole =
        AwsAssumeRoleAuthentication.newBuilder().setRoleArn(require(auth, "role_arn", "--auth"));
    setOptional(auth, "external_id", assumeRole::setExternalId);
    setOptional(auth, "role_session_name", assumeRole::setRoleSessionName);
    return assumeRole;
  }

  private static AwsAccessKeySecret.Builder accessKeySecret(Map<String, String> credentials) {
    var secret =
        AwsAccessKeySecret.newBuilder()
            .setSecretAccessKey(require(credentials, "secret_access_key", "--cred"));
    setOptional(credentials, "session_token", secret::setSessionToken);
    return secret;
  }

  private static String require(Map<String, String> values, String key, String flag) {
    String value = take(values, key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing " + flag + " " + key + "=...");
    }
    return value;
  }

  private static String take(Map<String, String> values, String key) {
    String value = values.remove(key);
    return value == null ? null : Quotes.unquote(value);
  }

  private static void setOptional(
      Map<String, String> values, String key, java.util.function.Consumer<String> setter) {
    String value = take(values, key);
    if (value != null && !value.isBlank()) setter.accept(value);
  }

  private static void rejectUnknown(Map<String, String> values, String flag) {
    if (!values.isEmpty()) {
      throw new IllegalArgumentException("Unsupported " + flag + " keys: " + values.keySet());
    }
  }

  private record ParsedAuthentication(
      CatalogAuthentication authentication, CatalogIntegrationCredentials credentials) {}

  private static CatalogIntegrationSpec integrationUpdateSpec(List<String> args) {
    var b = CatalogIntegrationSpec.newBuilder();
    if (args.contains("--display")) b.setDisplayName(requiredFlagValue(args, "--display"));
    if (args.contains("--uri")) b.setCatalogUri(requiredFlagValue(args, "--uri"));
    if (args.contains("--props")) b.putAllProperties(CliUtils.parseKeyValueList(args, "--props"));
    return b.build();
  }

  private static FieldMask integrationUpdateMask(List<String> args) {
    var paths = new ArrayList<String>();
    addIfPresent(paths, args, "--display", "display_name");
    addIfPresent(paths, args, "--uri", "catalog_uri");
    addIfPresent(paths, args, "--props", "properties");
    return FieldMask.newBuilder().addAllPaths(paths).build();
  }

  private static CatalogOverlaySpec overlaySpec(
      String name, ResourceId integrationId, ResourceId catalogId, List<String> args) {
    return CatalogOverlaySpec.newBuilder()
        .setDisplayName(name)
        .setIntegrationId(integrationId)
        .setCatalogId(catalogId)
        .addAllIncludeNamespaces(paths(args, "--include"))
        .addAllExcludeNamespaces(paths(args, "--exclude"))
        .build();
  }

  private static CatalogOverlaySpec overlayUpdateSpec(List<String> args) {
    var b = CatalogOverlaySpec.newBuilder();
    if (args.contains("--display")) b.setDisplayName(requiredFlagValue(args, "--display"));
    if (args.contains("--include")) b.addAllIncludeNamespaces(paths(args, "--include"));
    if (args.contains("--exclude")) b.addAllExcludeNamespaces(paths(args, "--exclude"));
    return b.build();
  }

  private static FieldMask overlayUpdateMask(List<String> args) {
    var paths = new ArrayList<String>();
    addIfPresent(paths, args, "--display", "display_name");
    addIfPresent(paths, args, "--include", "include_namespaces");
    addIfPresent(paths, args, "--exclude", "exclude_namespaces");
    return FieldMask.newBuilder().addAllPaths(paths).build();
  }

  private static List<NamespacePath> paths(List<String> args, String flag) {
    String value = args.contains(flag) ? requiredFlagValue(args, flag) : "";
    if (value.isBlank()) return List.of();
    return CliUtils.csvList(value).stream()
        .map(
            path ->
                NamespacePath.newBuilder().addAllSegments(FQNameParserUtil.segments(path)).build())
        .toList();
  }

  private static void listIntegrations(
      PrintStream out, CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations) {
    printIntegrationHeader(out);
    CliArgs.forEachPage(
        DEFAULT_PAGE_SIZE,
        page ->
            integrations.listCatalogIntegrations(
                ListCatalogIntegrationsRequest.newBuilder().setPage(page).build()),
        response ->
            response.getEntriesList().stream().map(entry -> entry.getIntegration()).toList(),
        response -> response.hasPage() ? response.getPage().getNextPageToken() : "",
        rows -> printIntegrationRows(rows, out));
  }

  private static void listOverlays(
      List<String> args,
      PrintStream out,
      CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations,
      CatalogOverlaysGrpc.CatalogOverlaysBlockingStub overlays,
      Supplier<String> accountId) {
    ResourceId filter = null;
    if (args.contains("--integration")) {
      filter =
          resolveIntegration(
              CliArgs.parseStringFlag(args, "--integration", ""), integrations, accountId);
    }
    ResourceId integrationFilter = filter;
    printOverlayHeader(out);
    CliArgs.forEachPage(
        DEFAULT_PAGE_SIZE,
        page -> {
          var request = ListCatalogOverlaysRequest.newBuilder().setPage(page);
          if (integrationFilter != null) request.setIntegrationId(integrationFilter);
          return overlays.listCatalogOverlays(request.build());
        },
        response -> response.getEntriesList().stream().map(entry -> entry.getOverlay()).toList(),
        response -> response.hasPage() ? response.getPage().getNextPageToken() : "",
        rows -> printOverlayRows(rows, out));
  }

  private static ResourceId resolveIntegration(
      String token,
      CatalogIntegrationsGrpc.CatalogIntegrationsBlockingStub integrations,
      Supplier<String> accountId) {
    String value = Quotes.unquote(token);
    if (CliUtils.looksLikeUuid(value)) {
      return rid(value, ResourceKind.RK_CATALOG_INTEGRATION, accountId);
    }
    return integrations
        .getCatalogIntegration(
            GetCatalogIntegrationRequest.newBuilder().setDisplayName(value).build())
        .getIntegration()
        .getResourceId();
  }

  private static ResourceId resolveOverlay(
      String token,
      CatalogOverlaysGrpc.CatalogOverlaysBlockingStub overlays,
      Supplier<String> accountId) {
    String value = Quotes.unquote(token);
    if (CliUtils.looksLikeUuid(value)) {
      return rid(value, ResourceKind.RK_CATALOG_OVERLAY, accountId);
    }
    return overlays
        .getCatalogOverlay(GetCatalogOverlayRequest.newBuilder().setDisplayName(value).build())
        .getOverlay()
        .getResourceId();
  }

  private static GetCatalogIntegrationRequest integrationSelector(
      String token, Supplier<String> accountId) {
    String value = Quotes.unquote(token);
    var request = GetCatalogIntegrationRequest.newBuilder();
    if (CliUtils.looksLikeUuid(value)) {
      request.setIntegrationId(rid(value, ResourceKind.RK_CATALOG_INTEGRATION, accountId));
    } else {
      request.setDisplayName(value);
    }
    return request.build();
  }

  private static GetCatalogOverlayRequest overlaySelector(
      String token, Supplier<String> accountId) {
    String value = Quotes.unquote(token);
    var request = GetCatalogOverlayRequest.newBuilder();
    if (CliUtils.looksLikeUuid(value)) {
      request.setOverlayId(rid(value, ResourceKind.RK_CATALOG_OVERLAY, accountId));
    } else {
      request.setDisplayName(value);
    }
    return request.build();
  }

  private static ResourceId rid(
      String id, ResourceKind kind, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank())
      throw new IllegalStateException("No account set. Use: account <accountId>");
    return ResourceId.newBuilder().setAccountId(accountId).setKind(kind).setId(id).build();
  }

  private static CatalogIntegrationType parseIntegrationType(String value) {
    return switch (normalized(value)) {
      case "ICEBERG_REST", "ICEBERG-REST", "ICEBERG" -> CatalogIntegrationType.CIT_ICEBERG_REST;
      case "UNITY" -> CatalogIntegrationType.CIT_UNITY;
      default -> throw new IllegalArgumentException("Unknown integration type: " + value);
    };
  }

  private static String normalized(String value) {
    return Quotes.unquote(value).trim().toUpperCase(Locale.ROOT);
  }

  private static void addIfPresent(
      List<String> paths, List<String> args, String flag, String path) {
    if (args.contains(flag)) paths.add(path);
  }

  private static String requiredFlagValue(List<String> args, String flag) {
    int index = args.indexOf(flag);
    if (index + 1 >= args.size() || args.get(index + 1).startsWith("--")) {
      throw new IllegalArgumentException("Missing value for " + flag);
    }
    return Quotes.unquote(args.get(index + 1));
  }

  private static void printIntegrations(List<CatalogIntegration> rows, PrintStream out) {
    printIntegrationHeader(out);
    printIntegrationRows(rows, out);
  }

  private static void printIntegrationHeader(PrintStream out) {
    out.printf("%-36s  %-24s  %-14s  %s%n", "INTEGRATION_ID", "NAME", "TYPE", "URI");
  }

  private static void printIntegrationRows(List<CatalogIntegration> rows, PrintStream out) {
    for (var row : rows) {
      out.printf(
          "%-36s  %-24s  %-14s  %s%n",
          CliUtils.rid(row.getResourceId()),
          row.getDisplayName(),
          row.getType().name().replaceFirst("^CIT_", ""),
          row.getCatalogUri());
    }
  }

  private static void printOverlays(List<CatalogOverlay> rows, PrintStream out) {
    printOverlayHeader(out);
    printOverlayRows(rows, out);
  }

  private static void printOverlayHeader(PrintStream out) {
    out.printf(
        "%-36s  %-24s  %-36s  %-36s  %-24s  %s%n",
        "OVERLAY_ID",
        "NAME",
        "INTEGRATION_ID",
        "CATALOG_ID",
        "INCLUDE_NAMESPACES",
        "EXCLUDE_NAMESPACES");
  }

  private static void printOverlayRows(List<CatalogOverlay> rows, PrintStream out) {
    for (var row : rows) {
      out.printf(
          "%-36s  %-24s  %-36s  %-36s  %-24s  %s%n",
          CliUtils.rid(row.getResourceId()),
          row.getDisplayName(),
          CliUtils.rid(row.getIntegrationId()),
          CliUtils.rid(row.getCatalogId()),
          formatNamespacePaths(row.getIncludeNamespacesList()),
          formatNamespacePaths(row.getExcludeNamespacesList()));
    }
  }

  private static String formatNamespacePaths(List<NamespacePath> paths) {
    return paths.stream()
        .map(path -> String.join(".", path.getSegmentsList()))
        .collect(java.util.stream.Collectors.joining(",", "[", "]"));
  }
}
