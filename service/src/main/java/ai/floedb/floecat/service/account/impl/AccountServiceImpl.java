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
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;

@GrpcService
public class AccountServiceImpl extends BaseServiceImpl implements AccountService {
  @Inject AccountRepository accountRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject StorageAuthorityRepository storageAuthorityRepo;
  @Inject TransactionRepository transactionRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject RecursiveResourceDropper recursiveDropper;
  @Inject DefaultCredentialResolver credentialResolver;
  @Inject SecretsManager secretsManager;
  @Inject MarkerStore markerStore;

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
                      (accountId == null || accountId.isBlank())
                          ? IdempotencyGuard.PLATFORM_SCOPE
                          : accountId;
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
    var deleteTarget = new AtomicReference<AccountDeleteTarget>();

    // Both paths below run the account's whole teardown — every catalog, namespace, table, view,
    // snapshot prefix and connector — as blocking storage I/O, and a lost delete CAS now raises a
    // retryable abort rather than reporting a silent success, so the body can run more than once.
    // run() subscribes on the Mutiny default executor and the retry re-subscribes through it, so no
    // attempt lands on the Vert.x event loop. That matters more here than elsewhere: cleanup runs
    // after the account pointer is already gone, so an attempt that dies mid-way leaves whatever it
    // did not reach orphaned, with nothing left to enumerate it.
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
                    if (safe.getPointerVersion() != 0L) {
                      // The pointer reappeared between the two reads, so a different account holds
                      // this id now. Neither answer below is safe for it: a precondition describes
                      // the account that is gone, and the sweep would tear down the live one's
                      // resources.
                      throw accountReappeared(accountId, corr);
                    }
                    pinDeleteTarget(deleteTarget, safe, null, accountId, corr);
                    // Provably absent. Cleanup runs after the pointer is removed and can die
                    // part-way, and this is the path a retry of such a delete lands on — returning
                    // success without finishing the job is what makes those orphans permanent, so
                    // sweep again. Teardown is idempotent: everything it removes is deleted
                    // unconditionally and re-scans find nothing on a second pass.
                    //
                    // The caller's precondition is deliberately not consulted here. It described a
                    // pointer that no longer exists, so it cannot be satisfied by anything — and
                    // the
                    // guard it used to drive answered NOT_FOUND to a conditional caller and swept
                    // for
                    // an unconditional one, deciding between "never existed" and "I removed it on
                    // an
                    // earlier attempt" from a fact about the request rather than about the account.
                    // Those two are indistinguishable from here, and the first answer is the one a
                    // teardown scheduler is most likely to read as "done" and stop. So an absent
                    // account is reported gone, and the sweep runs either way.
                    cleanupAccountResources(accountId, corr);
                    return DeleteAccountResponse.newBuilder().setMeta(safe).build();
                  }
                  pinDeleteTarget(
                      deleteTarget, meta, accountInstanceCreatedAt(meta), accountId, corr);

                  // A meaningful precondition applies to the live pointer we just read. Enforce it
                  // now; after this point a competing delete that makes the pointer absent follows
                  // the documented idempotent "already gone" contract rather than NOT_FOUND.
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());
                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          ai.floedb.floecat.common.rpc.Precondition.getDefaultInstance(),
                          expected -> accountRepo.deleteWithPrecondition(accountId, expected),
                          () -> accountRepo.metaForSafe(accountId),
                          corr,
                          "account",
                          Map.of("id", accountId.getId()));

                  if (deleteEstablishedAbsence(meta, out)) {
                    cleanupAccountResources(accountId, corr);
                  }

                  return DeleteAccountResponse.newBuilder().setMeta(out).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /**
   * Keeps every retry authorized against the account instance observed by the first attempt.
   * Absence is an expected later state after that instance's pointer has been removed; a different
   * live pointer is not. Without this pin, a storage failure during cleanup lets runWithRetry
   * re-enter through a newly created account and delete it as though it were the original target.
   */
  record AccountDeleteTarget(MutationMeta meta, Timestamp createdAt) {}

  Timestamp accountInstanceCreatedAt(MutationMeta meta) {
    if (meta.getPointerVersion() == 0L || meta.getBlobUri().isBlank()) {
      return null;
    }
    try {
      return accountRepo
          .getByBlobUri(meta.getBlobUri())
          .filter(Account::hasCreatedAt)
          .map(Account::getCreatedAt)
          .orElse(null);
    } catch (BaseResourceRepository.CorruptionException corrupt) {
      // The retry fence deliberately falls back to the exact pointer identity when the blob
      // cannot supply created_at. Do not make that documented corrupt-account delete path
      // unreachable by allowing the parse failure to escape before pinDeleteTarget.
      CLEANUP_LOG.warnf(
          corrupt,
          "delete_account_identity_unreadable pointer_key=%s blob_uri=%s",
          meta.getPointerKey(),
          meta.getBlobUri());
      return null;
    }
  }

  void pinDeleteTarget(
      AtomicReference<AccountDeleteTarget> target,
      MutationMeta observed,
      Timestamp observedCreatedAt,
      ResourceId accountId,
      String correlationId) {
    var pinned = target.get();
    if (pinned == null) {
      if (target.compareAndSet(null, new AccountDeleteTarget(observed, observedCreatedAt))) {
        return;
      }
      pinned = target.get();
    }
    if (observed.getPointerVersion() == 0L) {
      return;
    }
    if (pinned.meta().getPointerVersion() == 0L) {
      throw accountReappeared(accountId, correlationId);
    }
    if (pinned.createdAt() != null && observedCreatedAt != null) {
      if (!pinned.createdAt().equals(observedCreatedAt)) {
        throw accountReappeared(accountId, correlationId);
      }
      return;
    }
    // Corrupt legacy account blobs have no readable created_at identity. Retain the strict
    // pointer pin for those rather than weakening the account-reuse safety guarantee.
    if (pinned.meta().getPointerVersion() != observed.getPointerVersion()
        || !pinned.meta().getBlobUri().equals(observed.getBlobUri())
        || !pinned.meta().getEtag().equals(observed.getEtag())) {
      throw accountReappeared(accountId, correlationId);
    }
  }

  static boolean deleteEstablishedAbsence(MutationMeta before, MutationMeta outcome) {
    return outcome.getPointerVersion() == 0L
        || (outcome.getPointerVersion() == before.getPointerVersion()
            && outcome.getEtag().equals(before.getEtag()));
  }

  /**
   * Removes everything the account owned, having established that its pointer is gone.
   *
   * <p>Every step enumerates by an account id the caller supplied, and those are reusable: nothing
   * stops the same id being created again while this runs, and Floe's own teardown leaves that
   * window wide open — {@code AccountService#gcDeletedAccounts} advances an account to DELETED only
   * once floecat has answered, so an {@code undeleteAccount} racing this sweep is still permitted
   * for as long as the sweep takes. A recreate landing mid-sweep would then have the new account's
   * catalogs, namespaces, tables and views destroyed by a sweep authorized against the account that
   * used to hold the id.
   *
   * <p>So the absence this sweep rests on travels with it as a guard, in the same all-or-nothing
   * batch as each removal, rather than being re-read between steps: a recreate cannot slip between
   * a check and the delete it authorized, because there is no gap between them left to slip into.
   *
   * <p>That makes each removal safe; it does not by itself make the sweep complete. Completeness
   * rests on the other half of the same fence — a catalog or connector create pins the account
   * pointer in its own publish batch, see {@link
   * ai.floedb.floecat.service.repo.util.MarkerStore#accountLiveGuard} — so a create authorized
   * before this delete cannot pause and then commit behind the sweep that already passed its
   * prefix. Absence therefore proves what this sweep needs it to prove: that nothing new can appear
   * under the id while it runs.
   */
  void cleanupAccountResources(ResourceId accountId, String correlationId) {
    var accountKey = accountId.getId();
    var summary = new AccountCleanupSummary(accountKey);
    CLEANUP_LOG.infof("account_delete_cleanup_start account_id=%s", accountKey);
    var accountGone =
        markerStore.pointerAbsentGuard(
            "account " + accountKey, Keys.accountPointerById(accountKey));
    try {
      cleanupConnectors(accountId, summary, accountGone);
      cleanupStorageAuthorities(accountId, summary, accountGone);
      cleanupCatalogs(accountKey, summary, accountGone);
      // Deleting a resource whose blob cannot be read removes its canonical pointer but not the
      // by-name row, because the name it indexes lives in that blob. Sweep those two index families
      // a versioned row at a time, with accountGone in every delete batch. Account ids are
      // reusable, so a read before a bulk prefix delete would not authorize the rows it eventually
      // removes.
      var deleteProgress =
          new BaseResourceRepository.GuardedDeleteProgress(summary.hasPriorWrite());
      var transactionCleanup =
          transactionRepo.deleteAccountResources(accountKey, accountGone, deleteProgress);
      summary.transactionPointerRowsDeleted = transactionCleanup.pointersDeleted();
      summary.transactionBlobsDeleted = transactionCleanup.blobsDeleted();
      summary.residualIndexRowsDeleted =
          connectorRepo.deleteResidualRows(accountKey, accountGone, deleteProgress)
              + storageAuthorityRepo.deleteResidualRows(accountKey, accountGone, deleteProgress)
              + catalogRepo.deleteResidualRows(accountKey, accountGone, deleteProgress);
      // A connector or storage-authority pointer can disappear before its external secret delete
      // completes. The staged cleanup handles survive pointer deletion, so this pass can finish
      // tasks left by a crash and tasks whose pointer only the residual sweep managed to remove.
      connectorRepo.forEachCredentialCleanup(
          accountKey,
          cleanup -> cleanupConnectorCredential(accountKey, cleanup, summary, accountGone));
      storageAuthorityRepo.forEachCredentialCleanup(
          accountKey, cleanup -> cleanupStorageAuthorityCredential(cleanup, summary, accountGone));
      // And the children markers, which the walks above only reach by identity: a resource whose
      // canonical pointer was already gone is never enumerated, so its marker is never removed, and
      // no other sweep covers either marker family. Guarded properly rather than re-read, and the
      // asymmetry with the step above is deliberate: a marker is a fence, so taking a live
      // namespace's marker orphans the next table published into it rather than leaving something
      // repairable. See MarkerStore#deleteAccountMarkers.
      summary.markerRowsDeleted =
          markerStore.deleteAccountMarkers(accountKey, accountGone, deleteProgress);
      var idempotencyCleanup =
          idempotencyStore.deleteAccountResources(accountKey, accountGone, deleteProgress);
      summary.idempotencyPointerRowsDeleted = idempotencyCleanup.pointersDeleted();
      summary.idempotencyBlobsDeleted = idempotencyCleanup.blobsDeleted();
      CLEANUP_LOG.infof(
          "account_delete_cleanup_complete account_id=%s connectors=%d connector_credential_deletes=%d storage_authorities=%d storage_authority_credential_deletes=%d catalogs=%d namespaces=%d tables=%d views=%d snapshot_pointer_rows=%d transaction_pointer_rows=%d transaction_blobs=%d residual_index_rows=%d marker_rows=%d idempotency_pointer_rows=%d idempotency_blobs=%d",
          summary.accountId,
          summary.connectorsDeleted,
          summary.credentialsDeleted,
          summary.storageAuthoritiesDeleted,
          summary.storageAuthorityCredentialsDeleted,
          summary.catalogsDeleted,
          summary.namespacesDeleted,
          summary.tablesDeleted,
          summary.viewsDeleted,
          summary.snapshotPointerRowsDeleted,
          summary.transactionPointerRowsDeleted,
          summary.transactionBlobsDeleted,
          summary.residualIndexRowsDeleted,
          summary.markerRowsDeleted,
          summary.idempotencyPointerRowsDeleted,
          summary.idempotencyBlobsDeleted);
    } catch (BaseResourceRepository.BatchGuardFailedException reappeared) {
      // Child-publish guards also participate in teardown. Their failure is retryable: the next
      // attempt re-scans the namespace or catalog and removes the child that raced this one. Only
      // accountGone breaking means the id itself is live again and must be returned to the caller.
      if (accountGone.reevaluate() != BatchGuard.Outcome.BROKEN) {
        CLEANUP_LOG.infof(
            reappeared,
            "account_delete_cleanup_child_publish_race account_id=%s retryable=true",
            accountKey);
        throw reappeared;
      }
      // Deliberately not retryable: a retry re-reads, finds a live account, and deletes it — the
      // very account this abort exists to protect. Nothing internal can tell an undelete from a
      // fresh create, so the decision goes back to the caller that knows. Floe's GC treats a non-OK
      // answer as a failure to retry on its next pass, and by then its own state gate has the
      // answer: an account that was undeleted is no longer eligible and is never asked for again.
      //
      // The cost is that whatever this sweep had not reached is orphaned, with the pointer gone and
      // nothing left to enumerate it. That is the lesser of the two: the alternative is destroying
      // a
      // live account's data, and id reuse means no sweep can tell which rows belong to which
      // instance.
      CLEANUP_LOG.errorf(
          reappeared,
          "account_delete_cleanup_aborted_reappeared account_id=%s connectors=%d catalogs=%d"
              + " namespaces=%d tables=%d views=%d retryable=false",
          accountKey,
          summary.connectorsDeleted,
          summary.catalogsDeleted,
          summary.namespacesDeleted,
          summary.tablesDeleted,
          summary.viewsDeleted);
      throw accountReappeared(accountId, correlationId);
    } catch (RuntimeException e) {
      CLEANUP_LOG.errorf(e, "account_delete_cleanup_failed account_id=%s", accountKey);
      throw e;
    }
  }

  /**
   * The account id this delete was authorized against is live again, so the operation has no safe
   * way to continue. Not retryable — see the handler in {@link #cleanupAccountResources}.
   */
  private RuntimeException accountReappeared(ResourceId accountId, String correlationId) {
    return GrpcErrors.conflict(
        correlationId, ACCOUNT_REAPPEARED_DURING_DELETE, Map.of("id", accountId.getId()));
  }

  // Resource sweeps are driven from pointer rows rather than from the blob-parsing list() calls,
  // for the same reason the namespace walk is (see cleanupCatalog): this runs after the account
  // pointer is gone, and a retry would fail on the same unreadable blob. One truncated connector,
  // storage-authority, or catalog blob must not strand the remaining account resources. Identity is
  // all these sweeps need, and the repositories' delete already tolerates an unreadable blob.
  private void cleanupConnectors(
      ResourceId account, AccountCleanupSummary summary, BatchGuard accountGone) {
    String accountId = account.getId();
    connectorRepo.forEachId(
        accountId,
        connectorId -> {
          CLEANUP_LOG.infof(
              "account_delete_cleanup_connector account_id=%s connector_id=%s",
              accountId, connectorId.getId());
          // Stage a durable handle, then remove the pointer with accountGone before touching the
          // external secret. If the account reappears before the guarded delete, the pointer stays
          // and the handle cannot be claimed. If it reappears afterwards, the old connector is
          // already gone; connector ids are server-generated, so a replacement cannot acquire this
          // secret key. A failure after pointer removal leaves the handle for the residual pass (or
          // the next DeleteAccount attempt) to retry.
          var credentialCleanups = connectorRepo.prepareCredentialCleanup(connectorId);
          var credentialReady = connectorRepo.credentialCleanupReadyGuard(connectorId);
          boolean removed =
              connectorRepo.delete(connectorId, BatchGuard.all(accountGone, credentialReady));
          if (!removed && connectorRepo.metaForSafe(connectorId).getPointerVersion() != 0L) {
            throw new BaseResourceRepository.AbortRetryableException(
                "connector survived account teardown delete: " + connectorId.getId());
          }
          if (removed) {
            summary.connectorsDeleted++;
          }
          for (var cleanup : credentialCleanups) {
            cleanupConnectorCredential(accountId, cleanup, summary, accountGone);
          }
        });
  }

  private void cleanupConnectorCredential(
      String accountId,
      ConnectorRepository.CredentialCleanup cleanup,
      AccountCleanupSummary summary,
      BatchGuard accountGone) {
    var claimed = connectorRepo.claimCredentialCleanup(cleanup, accountGone);
    if (claimed.isEmpty()) {
      return;
    }
    credentialResolver.delete(accountId, cleanup.credentialId());
    summary.credentialsDeleted++;
    connectorRepo.completeCredentialCleanup(claimed.get());
  }

  private void cleanupStorageAuthorities(
      ResourceId account, AccountCleanupSummary summary, BatchGuard accountGone) {
    storageAuthorityRepo.forEachId(
        account.getId(),
        authorityId -> {
          CLEANUP_LOG.infof(
              "account_delete_cleanup_storage_authority account_id=%s authority_id=%s",
              account.getId(), authorityId.getId());
          var cleanup = storageAuthorityRepo.prepareCredentialCleanup(authorityId);
          var credentialReady = storageAuthorityRepo.credentialCleanupReadyGuard(authorityId);
          boolean removed =
              storageAuthorityRepo.delete(
                  authorityId, BatchGuard.all(accountGone, credentialReady));
          if (!removed && storageAuthorityRepo.metaForSafe(authorityId).getPointerVersion() != 0L) {
            throw new BaseResourceRepository.AbortRetryableException(
                "storage authority survived account teardown delete: " + authorityId.getId());
          }
          if (removed) {
            summary.storageAuthoritiesDeleted++;
          }
          cleanupStorageAuthorityCredential(cleanup, summary, accountGone);
        });
  }

  private void cleanupStorageAuthorityCredential(
      StorageAuthorityRepository.CredentialCleanup cleanup,
      AccountCleanupSummary summary,
      BatchGuard accountGone) {
    var claimed = storageAuthorityRepo.claimCredentialCleanup(cleanup, accountGone);
    if (claimed.isEmpty()) {
      return;
    }
    var authorityId = cleanup.authorityId();
    secretsManager.delete(
        authorityId.getAccountId(),
        StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE,
        authorityId.getId());
    summary.storageAuthorityCredentialsDeleted++;
    storageAuthorityRepo.completeCredentialCleanup(claimed.get());
  }

  private void cleanupCatalogs(
      String accountId, AccountCleanupSummary summary, BatchGuard accountGone) {
    catalogRepo.forEachRecoverableId(
        accountId, catalogId -> cleanupCatalog(catalogId, summary, accountGone));
  }

  private void cleanupCatalog(
      ResourceId catalogId, AccountCleanupSummary summary, BatchGuard accountGone) {
    CLEANUP_LOG.infof(
        "account_delete_cleanup_catalog account_id=%s catalog_id=%s",
        catalogId.getAccountId(), catalogId.getId());
    // Driven from pointer rows, deepest-first, in one streamed pass: a namespace whose blob cannot
    // be
    // parsed must still be torn down. Nothing here may hold the catalog's namespaces in memory;
    // child-publish fence failures retry this streamed scan from the beginning.
    long catalogMarkerVersion = markerStore.catalogMarkerVersion(catalogId);
    var dropped =
        recursiveDropper.dropCatalogNamespaces(catalogId.getAccountId(), catalogId, accountGone);
    summary.namespacesDeleted += dropped.namespacesDeleted;
    summary.tablesDeleted += dropped.tablesDeleted;
    summary.viewsDeleted += dropped.viewsDeleted;
    summary.snapshotPointerRowsDeleted += dropped.snapshotPointerRowsDeleted;
    var catalogChildrenUnchanged =
        markerStore.catalogChildrenUnchangedGuard(catalogId, catalogMarkerVersion);
    boolean removed =
        catalogRepo.delete(catalogId, BatchGuard.all(accountGone, catalogChildrenUnchanged));
    boolean gone = removed || catalogRepo.metaForSafe(catalogId).getPointerVersion() == 0L;
    if (!gone) {
      // The catalog changed without publishing a namespace. Its pointer is still live, so removing
      // the marker or reporting teardown complete would reopen the very publish race it fences.
      throw new BaseResourceRepository.AbortRetryableException(
          "catalog survived account teardown delete: " + catalogId.getId());
    }
    // The residual sweep below cannot reach the catalog's children marker — it is scoped to the
    // by-id and by-name families, and that row is under neither. See
    // MarkerStore#deleteCatalogMarker.
    markerStore.deleteCatalogMarker(catalogId);
    metadataGraph.invalidate(catalogId);
    if (removed) {
      summary.catalogsDeleted++;
    }
  }

  private static final class AccountCleanupSummary {
    private final String accountId;
    private int connectorsDeleted;
    private int credentialsDeleted;
    private int storageAuthoritiesDeleted;
    private int storageAuthorityCredentialsDeleted;
    private int catalogsDeleted;
    private int namespacesDeleted;
    private int tablesDeleted;
    private int viewsDeleted;
    private int snapshotPointerRowsDeleted;
    private int transactionPointerRowsDeleted;
    private int transactionBlobsDeleted;
    private int residualIndexRowsDeleted;
    private int markerRowsDeleted;
    private int idempotencyPointerRowsDeleted;
    private int idempotencyBlobsDeleted;

    private AccountCleanupSummary(String accountId) {
      this.accountId = accountId;
    }

    private boolean hasPriorWrite() {
      return connectorsDeleted != 0
          || credentialsDeleted != 0
          || storageAuthoritiesDeleted != 0
          || storageAuthorityCredentialsDeleted != 0
          || catalogsDeleted != 0
          || namespacesDeleted != 0
          || tablesDeleted != 0
          || viewsDeleted != 0
          || snapshotPointerRowsDeleted != 0
          || transactionPointerRowsDeleted != 0
          || transactionBlobsDeleted != 0;
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
