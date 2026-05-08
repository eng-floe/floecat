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

package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.client.cli.util.CliUtils;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.storage.rpc.CreateStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.DeleteStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.GetStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ListStorageAuthoritiesRequest;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.StorageAuthoritySpec;
import ai.floedb.floecat.storage.rpc.UpdateStorageAuthorityRequest;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

final class StorageAuthorityCliSupport {
  private static final int DEFAULT_PAGE_SIZE = 100;

  private StorageAuthorityCliSupport() {}

  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub authorities,
      Supplier<String> getCurrentAccountId) {
    switch (command) {
      case "storage-authorities" -> {
        int pageSize = CliArgs.parseIntFlag(args, "--page-size", DEFAULT_PAGE_SIZE);
        printAuthorities(listAll(pageSize, authorities), out);
      }
      case "storage-authority" -> crud(args, out, authorities, getCurrentAccountId);
      default ->
          throw new IllegalArgumentException("Unsupported storage authority command: " + command);
    }
  }

  private static void crud(
      List<String> args,
      PrintStream out,
      StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub authorities,
      Supplier<String> getCurrentAccountId) {
    if (args.isEmpty()) {
      out.println("usage: storage-authority <create|get|list|update|delete> ...");
      return;
    }
    switch (args.get(0)) {
      case "list" -> {
        int pageSize = CliArgs.parseIntFlag(args, "--page-size", DEFAULT_PAGE_SIZE);
        printAuthorities(listAll(pageSize, authorities), out);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: storage-authority get <display_name|id>");
          return;
        }
        ResourceId authorityId = resolveId(args.get(1), authorities, getCurrentAccountId);
        var response =
            authorities.getStorageAuthority(
                GetStorageAuthorityRequest.newBuilder().setAuthorityId(authorityId).build());
        printAuthorities(List.of(response.getAuthority()), out);
      }
      case "create" -> {
        if (args.size() < 2) {
          out.println(
              "usage: storage-authority create <display_name> --location-prefix <uri-prefix>"
                  + " [--desc <text>] [--enabled true|false] [--type <type>]"
                  + " [--region <region>] [--endpoint <uri>] [--path-style-access true|false]"
                  + " [--assume-role-arn <arn>] [--assume-role-external-id <id>]"
                  + " [--assume-role-session-name <name>] [--duration-seconds <n>]"
                  + " [--cred-type <type>] [--cred k=v ...] [--cred-head k=v ...]");
          return;
        }
        String display = Quotes.unquote(args.get(1));
        StorageAuthoritySpec spec = buildSpec(display, args, false);
        var response =
            authorities.createStorageAuthority(
                CreateStorageAuthorityRequest.newBuilder().setSpec(spec).build());
        printAuthorities(List.of(response.getAuthority()), out);
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: storage-authority update <display_name|id> [--display <name>]"
                  + " [--location-prefix <uri-prefix>] [--desc <text>] [--enabled true|false]"
                  + " [--type <type>] [--region <region>] [--endpoint <uri>]"
                  + " [--path-style-access true|false] [--assume-role-arn <arn>]"
                  + " [--assume-role-external-id <id>] [--assume-role-session-name <name>]"
                  + " [--duration-seconds <n>] [--cred-type <type>] [--cred k=v ...]"
                  + " [--cred-head k=v ...] [--etag <etag>]");
          return;
        }
        ResourceId authorityId = resolveId(args.get(1), authorities, getCurrentAccountId);
        String display = Quotes.unquote(CliArgs.parseStringFlag(args, "--display", ""));
        StorageAuthority current =
            authorities
                .getStorageAuthority(
                    GetStorageAuthorityRequest.newBuilder().setAuthorityId(authorityId).build())
                .getAuthority();
        StorageAuthoritySpec spec =
            buildSpec(display.isBlank() ? current.getDisplayName() : display, args, true);
        var request =
            UpdateStorageAuthorityRequest.newBuilder()
                .setAuthorityId(authorityId)
                .setSpec(spec)
                .setUpdateMask(buildUpdateMask(args));
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) {
          request.setPrecondition(precondition);
        }
        printAuthorities(
            List.of(authorities.updateStorageAuthority(request.build()).getAuthority()), out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: storage-authority delete <display_name|id> [--etag <etag>]");
          return;
        }
        ResourceId authorityId = resolveId(args.get(1), authorities, getCurrentAccountId);
        var request = DeleteStorageAuthorityRequest.newBuilder().setAuthorityId(authorityId);
        var precondition = CliArgs.preconditionFromEtag(args);
        if (precondition != null) {
          request.setPrecondition(precondition);
        }
        authorities.deleteStorageAuthority(request.build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private static StorageAuthoritySpec buildSpec(
      String display, List<String> args, boolean updating) {
    String locationPrefix =
        Quotes.unquote(CliArgs.parseStringFlag(args, "--location-prefix", updating ? "" : ""));
    String description = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", ""));
    String enabledRaw =
        Quotes.unquote(CliArgs.parseStringFlag(args, "--enabled", updating ? "" : "true"));
    String type = Quotes.unquote(CliArgs.parseStringFlag(args, "--type", ""));
    String region = Quotes.unquote(CliArgs.parseStringFlag(args, "--region", ""));
    String endpoint = Quotes.unquote(CliArgs.parseStringFlag(args, "--endpoint", ""));
    String pathStyleRaw = Quotes.unquote(CliArgs.parseStringFlag(args, "--path-style-access", ""));
    String assumeRoleArn = Quotes.unquote(CliArgs.parseStringFlag(args, "--assume-role-arn", ""));
    String assumeRoleExternalId =
        Quotes.unquote(CliArgs.parseStringFlag(args, "--assume-role-external-id", ""));
    String assumeRoleSessionName =
        Quotes.unquote(CliArgs.parseStringFlag(args, "--assume-role-session-name", ""));
    int durationSeconds = CliArgs.parseIntFlag(args, "--duration-seconds", 0);
    String credType = Quotes.unquote(CliArgs.parseStringFlag(args, "--cred-type", ""));
    var credProps = CliUtils.parseKeyValueList(args, "--cred");
    var credHeaders = CliUtils.parseKeyValueList(args, "--cred-head");

    var spec = StorageAuthoritySpec.newBuilder().setDisplayName(display);
    if (!enabledRaw.isBlank()) {
      spec.setEnabled(Boolean.parseBoolean(enabledRaw));
    }
    if (!locationPrefix.isBlank()) {
      spec.setLocationPrefix(locationPrefix);
    }
    if (!description.isBlank()) {
      spec.setDescription(description);
    }
    if (!type.isBlank()) {
      spec.setType(type);
    }
    if (!region.isBlank()) {
      spec.setRegion(region);
    }
    if (!endpoint.isBlank()) {
      spec.setEndpoint(endpoint);
    }
    if (!pathStyleRaw.isBlank()) {
      spec.setPathStyleAccess(Boolean.parseBoolean(pathStyleRaw));
    }
    if (!assumeRoleArn.isBlank()) {
      spec.setAssumeRoleArn(assumeRoleArn);
    }
    if (!assumeRoleExternalId.isBlank()) {
      spec.setAssumeRoleExternalId(assumeRoleExternalId);
    }
    if (!assumeRoleSessionName.isBlank()) {
      spec.setAssumeRoleSessionName(assumeRoleSessionName);
    }
    if (durationSeconds > 0) {
      spec.setDurationSeconds(durationSeconds);
    }
    var credentials = AuthCredentialParser.buildCredentials(credType, credProps, credHeaders);
    if (credentials != null) {
      spec.setCredentials(credentials);
    }
    return spec.build();
  }

  private static com.google.protobuf.FieldMask buildUpdateMask(List<String> args) {
    List<String> paths = new ArrayList<>();
    if (args.contains("--display")) {
      paths.add("display_name");
    }
    if (args.contains("--location-prefix")) {
      paths.add("location_prefix");
    }
    if (args.contains("--desc")) {
      paths.add("description");
    }
    if (args.contains("--enabled")) {
      paths.add("enabled");
    }
    if (args.contains("--type")) {
      paths.add("type");
    }
    if (args.contains("--region")) {
      paths.add("region");
    }
    if (args.contains("--endpoint")) {
      paths.add("endpoint");
    }
    if (args.contains("--path-style-access")) {
      paths.add("path_style_access");
    }
    if (args.contains("--assume-role-arn")) {
      paths.add("assume_role_arn");
    }
    if (args.contains("--assume-role-external-id")) {
      paths.add("assume_role_external_id");
    }
    if (args.contains("--assume-role-session-name")) {
      paths.add("assume_role_session_name");
    }
    if (args.contains("--duration-seconds")) {
      paths.add("duration_seconds");
    }
    if (args.contains("--cred-type") || args.contains("--cred") || args.contains("--cred-head")) {
      paths.add("credentials");
    }
    return com.google.protobuf.FieldMask.newBuilder().addAllPaths(paths).build();
  }

  private static List<StorageAuthority> listAll(
      int pageSize, StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub authorities) {
    return CliArgs.collectPages(
        pageSize,
        pr ->
            authorities.listStorageAuthorities(
                ListStorageAuthoritiesRequest.newBuilder().setPage(pr).build()),
        r -> r.getAuthoritiesList(),
        r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
  }

  static ResourceId resolveId(
      String token,
      StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub authorities,
      Supplier<String> getCurrentAccountId) {
    String t = Quotes.unquote(token);
    if (CliUtils.looksLikeUuid(t)) {
      return rid(t, getCurrentAccountId);
    }
    var all = listAll(DEFAULT_PAGE_SIZE, authorities);
    var exact = all.stream().filter(a -> t.equals(a.getDisplayName())).toList();
    if (exact.size() == 1) {
      return exact.get(0).getResourceId();
    }
    var ci = all.stream().filter(a -> t.equalsIgnoreCase(a.getDisplayName())).toList();
    if (ci.size() == 1) {
      return ci.get(0).getResourceId();
    }
    if (exact.isEmpty() && ci.isEmpty()) {
      throw new IllegalArgumentException("Storage authority not found: " + t);
    }
    throw new IllegalArgumentException("Storage authority name is ambiguous: " + t);
  }

  private static ResourceId rid(String id, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
        .setId(id)
        .build();
  }

  private static void printAuthorities(List<StorageAuthority> authorities, PrintStream out) {
    out.printf(
        "%-40s %-24s %-6s %-4s %s%n", "AUTHORITY_ID", "DISPLAY_NAME", "TYPE", "ON", "PREFIX");
    for (StorageAuthority authority : authorities) {
      out.printf(
          "%-40s %-24s %-6s %-4s %s%n",
          CliUtils.rid(authority.getResourceId()),
          authority.getDisplayName(),
          authority.getType(),
          authority.getEnabled(),
          authority.getLocationPrefix());
    }
  }
}
