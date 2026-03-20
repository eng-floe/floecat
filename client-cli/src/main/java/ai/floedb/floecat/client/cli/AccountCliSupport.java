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

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.Timestamp;
import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** CLI support for the {@code account} command. */
final class AccountCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private AccountCliSupport() {}

  /**
   * Dispatches {@code account} subcommands.
   *
   * @param args tokens after {@code account}
   * @param out output stream
   * @param accounts gRPC account service stub
   * @param getCurrentAccountId returns the currently selected account ID (may be null/blank)
   * @param setCurrentAccountId called when the user selects or clears an account
   */
  static void handle(
      List<String> args,
      PrintStream out,
      AccountServiceGrpc.AccountServiceBlockingStub accounts,
      Supplier<String> getCurrentAccountId,
      Consumer<String> setCurrentAccountId) {
    if (args.isEmpty()) {
      String current = getCurrentAccountId.get();
      out.println(
          (current == null || current.isBlank()) ? "account: <not set>" : ("account: " + current));
      return;
    }
    String sub = args.get(0);
    List<String> tail = CliArgs.tail(args);
    switch (sub) {
      case "list" -> accountList(out, accounts);
      case "get" -> accountGet(tail, out, accounts);
      case "create" -> accountCreate(tail, out, accounts);
      case "delete" -> accountDelete(tail, out, accounts, getCurrentAccountId, setCurrentAccountId);
      default -> {
        String t = sub.trim();
        if (t.isEmpty()) {
          out.println("usage: account <accountId|display_name>");
          return;
        }
        String resolved = resolveAccountId(t, accounts);
        setCurrentAccountId.accept(resolved);
        out.println("account set: " + resolved);
      }
    }
  }

  private static void accountList(
      PrintStream out, AccountServiceGrpc.AccountServiceBlockingStub accounts) {
    List<Account> all =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> accounts.listAccounts(ListAccountsRequest.newBuilder().setPage(pr).build()),
            r -> r.getAccountsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printAccounts(all, out);
  }

  private static void accountGet(
      List<String> args, PrintStream out, AccountServiceGrpc.AccountServiceBlockingStub accounts) {
    if (args.isEmpty()) {
      out.println("usage: account get <id|display_name>");
      return;
    }
    String id = resolveAccountId(args.get(0), accounts);
    var resp =
        accounts.getAccount(
            GetAccountRequest.newBuilder()
                .setAccountId(ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_ACCOUNT))
                .build());
    printAccounts(List.of(resp.getAccount()), out);
  }

  private static void accountCreate(
      List<String> args, PrintStream out, AccountServiceGrpc.AccountServiceBlockingStub accounts) {
    if (args.isEmpty()) {
      out.println("usage: account create <display_name> [--desc <text>]");
      return;
    }
    String display = Shell.Quotes.unquote(args.get(0));
    String desc = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", null));
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(display)
            .setDescription(desc == null ? "" : desc)
            .build();
    var resp = accounts.createAccount(CreateAccountRequest.newBuilder().setSpec(spec).build());
    printAccounts(List.of(resp.getAccount()), out);
  }

  private static void accountDelete(
      List<String> args,
      PrintStream out,
      AccountServiceGrpc.AccountServiceBlockingStub accounts,
      Supplier<String> getCurrentAccountId,
      Consumer<String> setCurrentAccountId) {
    String current = getCurrentAccountId.get();
    String token = args.isEmpty() ? (current == null ? "" : current.trim()) : args.get(0);
    token = Shell.Quotes.unquote(token);
    if (token.isBlank()) {
      out.println("usage: account delete <id|display_name>");
      return;
    }
    String id = resolveAccountId(token, accounts);
    accounts.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_ACCOUNT))
            .build());
    if (current != null && current.trim().equals(id)) {
      setCurrentAccountId.accept(null);
    }
    out.println("account deleted: " + id);
  }

  static String resolveAccountId(
      String token, AccountServiceGrpc.AccountServiceBlockingStub accounts) {
    String value = Shell.Quotes.unquote(token == null ? "" : token).trim();
    if (value.isBlank()) {
      throw new IllegalArgumentException("account id/display name cannot be empty");
    }
    if (looksLikeUuid(value)) {
      return value;
    }
    List<Account> all =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> accounts.listAccounts(ListAccountsRequest.newBuilder().setPage(pr).build()),
            r -> r.getAccountsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    return all.stream()
        .filter(a -> value.equals(a.getDisplayName()))
        .map(a -> a.getResourceId().getId())
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("account not found by id/display name: " + value));
  }

  private static void printAccounts(List<Account> rows, PrintStream out) {
    out.printf(
        "%-40s  %-24s  %-24s  %s%n", "ACCOUNT_ID", "CREATED_AT", "DISPLAY_NAME", "DESCRIPTION");
    for (var a : rows) {
      out.printf(
          "%-40s  %-24s  %-24s  %s%n",
          rid(a.getResourceId()),
          ts(a.getCreatedAt()),
          Shell.Quotes.quoteIfNeeded(a.getDisplayName()),
          a.hasDescription() ? a.getDescription() : "");
    }
  }

  private static boolean looksLikeUuid(String s) {
    if (s == null) return false;
    return s.trim()
        .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  private static String rid(ResourceId id) {
    String s = (id == null) ? null : id.getId();
    return (s == null || s.isBlank()) ? "<no-id>" : s;
  }

  private static String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) return "-";
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }
}
