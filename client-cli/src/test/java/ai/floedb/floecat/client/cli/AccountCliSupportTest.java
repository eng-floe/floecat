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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.CreateAccountResponse;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountResponse;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountResponse;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.ListAccountsResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AccountCliSupportTest {

  private static final String UUID_1 = "00000000-0000-0000-0000-000000000001";
  private static final String UUID_2 = "00000000-0000-0000-0000-000000000002";

  private static Account account(String id, String displayName) {
    return Account.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId(id).build())
        .setDisplayName(displayName)
        .build();
  }

  // --- no args: print current account ---

  @Test
  void noArgsPrintsNotSetWhenNoAccount() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(List.of(), new PrintStream(buf), h.stub, () -> null, ignored -> {});
      assertTrue(buf.toString().contains("<not set>"));
    }
  }

  @Test
  void noArgsPrintsCurrentAccountId() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of(), new PrintStream(buf), h.stub, () -> UUID_1, ignored -> {});
      assertTrue(buf.toString().contains(UUID_1));
    }
  }

  // --- list ---

  @Test
  void listPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.service.accountsToList.add(account(UUID_1, "acme"));

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("list"), new PrintStream(buf), h.stub, () -> null, ignored -> {});

      String out = buf.toString();
      assertTrue(out.contains("ACCOUNT_ID"), "expected header");
      assertTrue(out.contains(UUID_1), "expected account id");
      assertTrue(out.contains("acme"), "expected display name");
    }
  }

  // --- get ---

  @Test
  void getByUuidCallsServiceAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.service.accountToReturn = account(UUID_1, "acme");

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("get", UUID_1), new PrintStream(buf), h.stub, () -> null, ignored -> {});

      assertEquals(1, h.service.getAccountCalls.get());
      assertEquals(UUID_1, h.service.lastGetRequest.getAccountId().getId());
      assertTrue(buf.toString().contains(UUID_1));
    }
  }

  @Test
  void getPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("get"), new PrintStream(buf), h.stub, () -> null, ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- create ---

  @Test
  void createSendsSpecAndPrintsResult() throws Exception {
    try (Harness h = new Harness()) {
      h.service.accountToReturn = account(UUID_2, "new-acct");

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("create", "new-acct", "--desc", "My account"),
          new PrintStream(buf),
          h.stub,
          () -> null,
          ignored -> {});

      assertEquals(1, h.service.createAccountCalls.get());
      assertEquals("new-acct", h.service.lastCreateRequest.getSpec().getDisplayName());
      assertEquals("My account", h.service.lastCreateRequest.getSpec().getDescription());
      assertTrue(buf.toString().contains(UUID_2));
    }
  }

  @Test
  void createPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("create"), new PrintStream(buf), h.stub, () -> null, ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- delete ---

  @Test
  void deleteByUuidCallsServiceAndPrintsConfirmation() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("delete", UUID_1), new PrintStream(buf), h.stub, () -> null, ignored -> {});

      assertEquals(1, h.service.deleteAccountCalls.get());
      assertEquals(UUID_1, h.service.lastDeleteRequest.getAccountId().getId());
      assertTrue(buf.toString().contains("account deleted"));
    }
  }

  @Test
  void deleteClearsCurrentAccountIdWhenMatches() throws Exception {
    try (Harness h = new Harness()) {
      AtomicReference<String> current = new AtomicReference<>(UUID_1);

      AccountCliSupport.handle(
          List.of("delete", UUID_1),
          new PrintStream(new ByteArrayOutputStream()),
          h.stub,
          current::get,
          current::set);

      assertNull(current.get(), "current account should be cleared after deleting it");
    }
  }

  @Test
  void deleteDoesNotClearCurrentAccountWhenDifferent() throws Exception {
    try (Harness h = new Harness()) {
      AtomicReference<String> current = new AtomicReference<>(UUID_2);

      AccountCliSupport.handle(
          List.of("delete", UUID_1),
          new PrintStream(new ByteArrayOutputStream()),
          h.stub,
          current::get,
          current::set);

      assertEquals(UUID_2, current.get(), "different current account should not be cleared");
    }
  }

  @Test
  void deletePrintsUsageWhenNoArgsAndNoCurrentAccount() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      AccountCliSupport.handle(
          List.of("delete"), new PrintStream(buf), h.stub, () -> null, ignored -> {});
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- select account by name/id (default case) ---

  @Test
  void bareUuidSetsCurrentAccount() throws Exception {
    try (Harness h = new Harness()) {
      AtomicReference<String> current = new AtomicReference<>("");
      AccountCliSupport.handle(
          List.of(UUID_1),
          new PrintStream(new ByteArrayOutputStream()),
          h.stub,
          current::get,
          current::set);
      assertEquals(UUID_1, current.get());
    }
  }

  @Test
  void bareDisplayNameResolvesAndSetsAccount() throws Exception {
    try (Harness h = new Harness()) {
      h.service.accountsToList.add(account(UUID_1, "acme"));
      AtomicReference<String> current = new AtomicReference<>("");

      AccountCliSupport.handle(
          List.of("acme"),
          new PrintStream(new ByteArrayOutputStream()),
          h.stub,
          current::get,
          current::set);

      assertEquals(UUID_1, current.get(), "should resolve display name to uuid");
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingAccountService service;
    final AccountServiceGrpc.AccountServiceBlockingStub stub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.service = new CapturingAccountService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(service)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.stub = AccountServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingAccountService
      extends AccountServiceGrpc.AccountServiceImplBase {

    final AtomicInteger getAccountCalls = new AtomicInteger();
    final AtomicInteger createAccountCalls = new AtomicInteger();
    final AtomicInteger deleteAccountCalls = new AtomicInteger();
    final List<Account> accountsToList = new ArrayList<>();
    Account accountToReturn = Account.getDefaultInstance();
    GetAccountRequest lastGetRequest;
    CreateAccountRequest lastCreateRequest;
    DeleteAccountRequest lastDeleteRequest;

    @Override
    public void listAccounts(
        ListAccountsRequest request, StreamObserver<ListAccountsResponse> responseObserver) {
      responseObserver.onNext(
          ListAccountsResponse.newBuilder().addAllAccounts(accountsToList).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getAccount(
        GetAccountRequest request, StreamObserver<GetAccountResponse> responseObserver) {
      getAccountCalls.incrementAndGet();
      lastGetRequest = request;
      responseObserver.onNext(GetAccountResponse.newBuilder().setAccount(accountToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void createAccount(
        CreateAccountRequest request, StreamObserver<CreateAccountResponse> responseObserver) {
      createAccountCalls.incrementAndGet();
      lastCreateRequest = request;
      responseObserver.onNext(
          CreateAccountResponse.newBuilder().setAccount(accountToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteAccount(
        DeleteAccountRequest request, StreamObserver<DeleteAccountResponse> responseObserver) {
      deleteAccountCalls.incrementAndGet();
      lastDeleteRequest = request;
      responseObserver.onNext(DeleteAccountResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
