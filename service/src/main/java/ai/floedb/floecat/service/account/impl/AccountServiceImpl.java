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

package ai.floedb.floecat.service.account.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

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
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.RecursiveResourceDropper;
import ai.floedb.floecat.service.common.AccountIds;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.credentials.DefaultCredentialResolver;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.jboss.logging.Logger;

@GrpcService
public class AccountServiceImpl extends BaseServiceImpl implements AccountService {
  @Inject AccountRepository accountRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject RecursiveResourceDropper recursiveDropper;
  @Inject DefaultCredentialResolver credentialResolver;

  private static final Set<String> ACCOUNT_MUTABLE_PATHS =
      Set.of("display_name", "description", "tags");

  private static final Logger LOG = Logger.getLogger(AccountService.class);
  private static final Logger CLEANUP_LOG = Logger.getLogger(AccountServiceImpl.class);

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
                        correlationId(), PAGE_TOKEN_INVALID, Map.of("page_token", pageIn.token));
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
                                      correlationId, ACCOUNT, Map.of("id", resourceId.getId())));

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
                  final var idempotencyAccount =
                      (accountId == null || accountId.isBlank()) ? "platform" : accountId;
                  authz.require(pc, "account.write");

                  final var tsNow = nowTs();

                  final var spec = request.getSpec();
                  final String rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  final String normName = normalizeName(rawName);

                  final String explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  final String idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  final var normalizedSpec = spec.toBuilder().setDisplayName(normName).build();
                  final byte[] fingerprint = canonicalFingerprint(normalizedSpec);

                  final var resourceId = resolveAccountId(request, accountId, corr);

                  final var desiredAccount =
                      Account.newBuilder()
                          .setResourceId(resourceId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCreatedAt(tsNow)
                          .putAllTags(spec.getTagsMap())
                          .build();

                  if (idempotencyKey == null) {
                    var existingOpt = accountRepo.getByName(normName);
                    if (existingOpt.isPresent()) {
                      throw GrpcErrors.alreadyExists(
                          corr, ACCOUNT_ALREADY_EXISTS, Map.of("display_name", normName));
                    }

                    accountRepo.create(desiredAccount);
                    var meta = accountRepo.metaForSafe(resourceId);
                    return CreateAccountResponse.newBuilder()
                        .setAccount(desiredAccount)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  idempotencyAccount,
                                  "CreateAccount",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    try {
                                      accountRepo.create(desiredAccount);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      var existingOpt = accountRepo.getByName(normName);
                                      if (existingOpt.isPresent()) {
                                        var existingSpec = specFromAccount(existingOpt.get());
                                        if (Arrays.equals(
                                            fingerprint, canonicalFingerprint(existingSpec))) {
                                          return new IdempotencyGuard.CreateResult<>(
                                              existingOpt.get(), existingOpt.get().getResourceId());
                                        }
                                      }
                                      throw GrpcErrors.alreadyExists(
                                          corr,
                                          ACCOUNT_ALREADY_EXISTS,
                                          Map.of("display_name", normName));
                                    }
                                    return new IdempotencyGuard.CreateResult<>(
                                        desiredAccount, resourceId);
                                  },
                                  (t) -> accountRepo.metaFor(t.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Account::parseFrom));

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

  private ResourceId resolveAccountId(
      CreateAccountRequest request, String principalAccountId, String corr) {
    if (request.hasAccountId()) {
      var candidate = ResourceId.newBuilder(request.getAccountId());
      if (candidate.getId().isBlank()) {
        throw GrpcErrors.invalidArgument(corr, ACCOUNT_ID_ID_REQUIRED, Map.of());
      }
      if (candidate.getKind() == ResourceKind.RK_UNSPECIFIED) {
        candidate.setKind(ResourceKind.RK_ACCOUNT);
      }
      if (candidate.getKind() != ResourceKind.RK_ACCOUNT) {
        throw GrpcErrors.invalidArgument(
            corr, ACCOUNT_ID_KIND_INVALID, Map.of("kind", candidate.getKind().name()));
      }
      if (candidate.getAccountId().isBlank()) {
        if (principalAccountId != null && !principalAccountId.isBlank()) {
          candidate.setAccountId(principalAccountId);
        } else {
          candidate.setAccountId(candidate.getId());
        }
      }
      return candidate.build();
    }

    final String accountUuid = AccountIds.randomAccountId();
    final String accountId =
        (principalAccountId == null || principalAccountId.isBlank())
            ? accountUuid
            : principalAccountId;
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(accountUuid)
        .setKind(ResourceKind.RK_ACCOUNT)
        .build();
  }

  @Override
  public Uni<UpdateAccountResponse> updateAccount(UpdateAccountRequest request) {
    var L = LogHelper.start(LOG, "UpdateAccount");

    return mapFailures(
            runWithRetry(
                () -> {
                  final var pc = principal.get();
                  final var corr = pc.getCorrelationId();
                  authz.require(pc, List.of("account.write", "account.delete"));

                  var accountId = request.getAccountId();
                  ensureKind(accountId, ResourceKind.RK_ACCOUNT, "account_id", corr);

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = normalizeMask(request.getUpdateMask());

                  var meta = accountRepo.metaFor(accountId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  var current =
                      accountRepo
                          .getById(accountId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, ACCOUNT, Map.of("id", accountId.getId())));

                  var desired = applyAccountSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = accountRepo.metaFor(accountId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && metaNoop.getPointerVersion() != meta.getPointerVersion()) {
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(metaNoop.getPointerVersion())));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateAccountResponse.newBuilder()
                        .setAccount(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  try {
                    boolean ok = accountRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = accountRepo.metaForSafe(accountId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.alreadyExists(
                        corr,
                        ACCOUNT_ALREADY_EXISTS,
                        Map.of("display_name", desired.getDisplayName()));
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = accountRepo.metaForSafe(accountId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        VERSION_MISMATCH,
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }

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
                  authz.require(pc, "account.delete");

                  var accountId = request.getAccountId();
                  ensureKind(accountId, ResourceKind.RK_ACCOUNT, "account_id", corr);

                  MutationMeta meta;
                  try {
                    meta = accountRepo.metaFor(accountId);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = accountRepo.metaForSafe(accountId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(corr, ACCOUNT, Map.of("id", accountId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, safe, request.getPrecondition());
                    return DeleteAccountResponse.newBuilder().setMeta(safe).build();
                  }

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected -> accountRepo.deleteWithPrecondition(accountId, expected),
                          () -> accountRepo.metaForSafe(accountId),
                          corr,
                          "account",
                          Map.of("id", accountId.getId()));

                  if (out.getPointerVersion() == meta.getPointerVersion()
                      && out.getEtag().equals(meta.getEtag())) {
                    cleanupAccountResources(accountId);
                  }

                  return DeleteAccountResponse.newBuilder().setMeta(out).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private void cleanupAccountResources(ResourceId accountId) {
    var accountKey = accountId.getId();
    var summary = new AccountCleanupSummary(accountKey);
    CLEANUP_LOG.infof("account_delete_cleanup_start account_id=%s", accountKey);
    try {
      cleanupConnectors(accountKey, summary);
      cleanupCatalogs(accountKey, summary);
      CLEANUP_LOG.infof(
          "account_delete_cleanup_complete account_id=%s connectors=%d credential_deletes=%d catalogs=%d namespaces=%d tables=%d views=%d snapshot_prefix_deletes=%d",
          summary.accountId,
          summary.connectorsDeleted,
          summary.credentialsDeleted,
          summary.catalogsDeleted,
          summary.namespacesDeleted,
          summary.tablesDeleted,
          summary.viewsDeleted,
          summary.snapshotPrefixesDeleted);
    } catch (RuntimeException e) {
      CLEANUP_LOG.errorf(e, "account_delete_cleanup_failed account_id=%s", accountKey);
      throw e;
    }
  }

  private void cleanupConnectors(String accountId, AccountCleanupSummary summary) {
    for (var connector :
        listAllPages((token, next) -> connectorRepo.list(accountId, 200, token, next))) {
      var connectorId = connector.getResourceId();
      CLEANUP_LOG.infof(
          "account_delete_cleanup_connector account_id=%s connector_id=%s",
          accountId, connectorId.getId());
      connectorRepo.delete(connectorId);
      credentialResolver.delete(accountId, connectorId.getId());
      summary.connectorsDeleted++;
      summary.credentialsDeleted++;
    }
  }

  private void cleanupCatalogs(String accountId, AccountCleanupSummary summary) {
    for (var catalog :
        listAllPages((token, next) -> catalogRepo.list(accountId, 200, token, next))) {
      cleanupCatalog(catalog, summary);
    }
  }

  private void cleanupCatalog(Catalog catalog, AccountCleanupSummary summary) {
    var catalogId = catalog.getResourceId();
    CLEANUP_LOG.infof(
        "account_delete_cleanup_catalog account_id=%s catalog_id=%s",
        catalogId.getAccountId(), catalogId.getId());
    for (var namespaceId :
        recursiveDropper.namespaceIds(catalogId.getAccountId(), catalogId.getId())) {
      // A previous tree may have removed this namespace as its descendant.
      recursiveDropper
          .dropNamespaceTree(namespaceId)
          .ifPresent(
              dropped -> {
                summary.namespacesDeleted += dropped.namespacesDeleted;
                summary.tablesDeleted += dropped.tablesDeleted;
                summary.viewsDeleted += dropped.viewsDeleted;
                summary.snapshotPrefixesDeleted += dropped.snapshotPrefixesDeleted;
              });
    }
    catalogRepo.delete(catalogId);
    metadataGraph.invalidate(catalogId);
    summary.catalogsDeleted++;
  }

  private <T> List<T> listAllPages(BiFunction<String, StringBuilder, List<T>> pageLoader) {
    var items = new ArrayList<T>();
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      items.addAll(pageLoader.apply(token, next));
      token = next.toString();
      if (token.isBlank()) {
        return items;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException("stagnant page token during account cleanup: " + token);
      }
    }
  }

  private static final class AccountCleanupSummary {
    private final String accountId;
    private int connectorsDeleted;
    private int credentialsDeleted;
    private int catalogsDeleted;
    private int namespacesDeleted;
    private int tablesDeleted;
    private int viewsDeleted;
    private int snapshotPrefixesDeleted;

    private AccountCleanupSummary(String accountId) {
      this.accountId = accountId;
    }
  }

  private Account applyAccountSpecPatch(
      Account current, AccountSpec spec, FieldMask mask, String corr) {
    mask = normalizeMask(mask);

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
    }

    for (var p : paths) {
      if (!ACCOUNT_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_PATH_INVALID, Map.of("path", p));
      }
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      var name = spec.getDisplayName();
      if (name == null || name.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, DISPLAY_NAME_REQUIRED, Map.of());
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

    if (maskTargets(mask, "tags")) {
      b.clearTags().putAllTags(spec.getTagsMap());
    }

    return b.build();
  }

  private static FieldMask normalizeMask(FieldMask mask) {
    if (mask == null) {
      return null;
    }
    var out = FieldMask.newBuilder();
    for (var p : mask.getPathsList()) {
      if (p == null) {
        continue;
      }
      var t = p.trim().toLowerCase();
      if (!t.isEmpty()) {
        out.addPaths(t);
      }
    }
    return out.build();
  }

  private static byte[] canonicalFingerprint(AccountSpec s) {
    return new Canonicalizer()
        .scalar("name", normalizeName(s.getDisplayName()))
        .scalar("description", s.getDescription())
        .map("tags", s.getTagsMap())
        .bytes();
  }

  private static AccountSpec specFromAccount(Account account) {
    return AccountSpec.newBuilder()
        .setDisplayName(normalizeName(account.getDisplayName()))
        .setDescription(account.getDescription())
        .putAllTags(account.getTagsMap())
        .build();
  }
}
