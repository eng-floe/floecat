package ai.floedb.floecat.service.account.impl;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountService;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.CreateAccountResponse;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountResponse;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountResponse;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.ListAccountsResponse;
import ai.floedb.floecat.account.rpc.UpdateAccountRequest;
import ai.floedb.floecat.account.rpc.UpdateAccountResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class AccountServiceImpl extends BaseServiceImpl implements AccountService {
  @Inject AccountRepository accountRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Set<String> ACCOUNT_MUTABLE_PATHS = Set.of("display_name", "description");

  private static final Logger LOG = Logger.getLogger(AccountService.class);

  @Override
  public Uni<ListAccountsResponse> listAccounts(ListAccountsRequest request) {
    var L = LogHelper.start(LOG, "ListAccounts");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "account.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();

                  List<Account> accounts;
                  try {
                    accounts = accountRepo.list(Math.max(1, pageIn.limit), pageIn.token, next);
                  } catch (IllegalArgumentException badToken) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", pageIn.token));
                  }

                  var page = MutationOps.pageOut(next.toString(), accountRepo.count());

                  return ListAccountsResponse.newBuilder()
                      .addAllAccounts(accounts)
                      .setPage(page)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  public Uni<GetAccountResponse> getAccount(GetAccountRequest request) {
    var L = LogHelper.start(LOG, "GetAccount");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var principalContext = principal.get();
                  final var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "account.read");

                  var resourceId = request.getAccountId();
                  ensureKind(resourceId, ResourceKind.RK_ACCOUNT, "account_id", correlationId);

                  var account =
                      accountRepo
                          .getById(resourceId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId, "account", Map.of("id", resourceId.getId())));

                  return GetAccountResponse.newBuilder().setAccount(account).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateAccountResponse> createAccount(CreateAccountRequest request) {
    var L = LogHelper.start(LOG, "CreateAccount");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  final var accountId = pc.getAccountId();
                  authz.require(pc, "account.write");

                  final var tsNow = nowTs();

                  final var spec = request.getSpec();
                  final String rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  final String normName = normalizeName(rawName);

                  final String explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  final String idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  final byte[] fingerprint = canonicalFingerprint(spec);

                  final String accountUuid =
                      deterministicUuid(
                          accountId,
                          "account",
                          Base64.getUrlEncoder().withoutPadding().encodeToString(fingerprint));

                  final var resourceId =
                      ResourceId.newBuilder()
                          .setAccountId(accountId)
                          .setId(accountUuid)
                          .setKind(ResourceKind.RK_ACCOUNT)
                          .build();

                  final var desiredAccount =
                      Account.newBuilder()
                          .setResourceId(resourceId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCreatedAt(tsNow)
                          .build();

                  if (idempotencyKey == null) {
                    var existingOpt = accountRepo.getByName(normName);
                    if (existingOpt.isPresent()) {
                      var existing = existingOpt.get();
                      var meta = accountRepo.metaForSafe(existing.getResourceId());
                      return CreateAccountResponse.newBuilder()
                          .setAccount(existing)
                          .setMeta(meta)
                          .build();
                    }

                    accountRepo.create(desiredAccount);
                    var meta = accountRepo.metaForSafe(resourceId);
                    return CreateAccountResponse.newBuilder()
                        .setAccount(desiredAccount)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      MutationOps.createProto(
                          accountId,
                          "CreateAccount",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            accountRepo.create(desiredAccount);
                            return new IdempotencyGuard.CreateResult<>(desiredAccount, resourceId);
                          },
                          (t) -> accountRepo.metaFor(t.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Account::parseFrom,
                          rec -> accountRepo.getById(rec.getResourceId()).isPresent());

                  return CreateAccountResponse.newBuilder()
                      .setAccount(result.body)
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateAccountResponse> updateAccount(UpdateAccountRequest request) {
    var L = LogHelper.start(LOG, "UpdateAccount");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  authz.require(pc, "account.write");

                  var accountId = request.getAccountId();
                  ensureKind(accountId, ResourceKind.RK_ACCOUNT, "account_id", corr);

                  var current =
                      accountRepo
                          .getById(accountId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "account", Map.of("id", accountId.getId())));

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = request.getUpdateMask();

                  var desired = applyAccountSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = accountRepo.metaForSafe(accountId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateAccountResponse.newBuilder()
                        .setAccount(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  MutationOps.updateWithPreconditions(
                      () -> accountRepo.metaFor(accountId),
                      request.getPrecondition(),
                      expected -> accountRepo.update(desired, expected),
                      () -> accountRepo.metaForSafe(accountId),
                      corr,
                      "account",
                      Map.of("display_name", desired.getDisplayName()));

                  var outMeta = accountRepo.metaForSafe(accountId);
                  var latest = accountRepo.getById(accountId).orElse(desired);
                  return UpdateAccountResponse.newBuilder()
                      .setAccount(latest)
                      .setMeta(outMeta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteAccountResponse> deleteAccount(DeleteAccountRequest request) {
    var L = LogHelper.start(LOG, "DeleteAccount");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  authz.require(pc, "account.write");

                  var accountId = request.getAccountId();
                  ensureKind(accountId, ResourceKind.RK_ACCOUNT, "account_id", corr);

                  if (catalogRepo.count(accountId.getId()) > 0) {
                    var cur = accountRepo.getById(accountId).orElse(null);
                    var name =
                        (cur != null && !cur.getDisplayName().isBlank())
                            ? cur.getDisplayName()
                            : accountId.getId();
                    throw GrpcErrors.conflict(
                        corr, "account.not_empty", Map.of("display_name", name));
                  }

                  var meta =
                      MutationOps.deleteWithPreconditions(
                          () -> accountRepo.metaFor(accountId),
                          request.getPrecondition(),
                          expected -> accountRepo.deleteWithPrecondition(accountId, expected),
                          () -> accountRepo.metaForSafe(accountId),
                          corr,
                          "account",
                          Map.of("id", accountId.getId()));

                  return DeleteAccountResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private Account applyAccountSpecPatch(
      Account current, AccountSpec spec, FieldMask mask, String corr) {

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }

    for (var p : paths) {
      if (!ACCOUNT_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, "update_mask.path.invalid", Map.of("path", p));
      }
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      var name = spec.getDisplayName();
      if (name == null || name.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.required", Map.of());
      }
      b.setDisplayName(name);
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) {
        b.setDescription(spec.getDescription());
      } else {
        b.clearDescription();
      }
    }

    return b.build();
  }

  private static byte[] canonicalFingerprint(AccountSpec s) {
    return new Canonicalizer().scalar("name", normalizeName(s.getDisplayName())).bytes();
  }
}
