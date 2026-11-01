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
import ai.floedb.floecat.account.rpc.AwsTrustConfiguration;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.CreateAccountResponse;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountResponse;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountResponse;
import ai.floedb.floecat.account.rpc.GetAwsTrustConfigurationRequest;
import ai.floedb.floecat.account.rpc.GetAwsTrustConfigurationResponse;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.ListAccountsResponse;
import ai.floedb.floecat.account.rpc.UpdateAccountRequest;
import ai.floedb.floecat.account.rpc.UpdateAccountResponse;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.AccountIds;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.arns.Arn;

@GrpcService
public class AccountServiceImpl extends BaseServiceImpl implements AccountService {
  @Inject AccountRepository accountRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject PointerStore pointerStore;
  @Inject AccountCredentialCleanup credentialCleanup;
  @Inject AccountAwsExternalIdProvider awsExternalIds;

  @ConfigProperty(name = "floecat.catalog-integrations.aws.service-principal-arn")
  Optional<String> awsServicePrincipalArn;

  private static final Set<String> ACCOUNT_MUTABLE_PATHS =
      Set.of("display_name", "description", "tags");

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
  public Uni<GetAwsTrustConfigurationResponse> getAwsTrustConfiguration(
      GetAwsTrustConfigurationRequest request) {
    var L = LogHelper.start(LOG, "GetAwsTrustConfiguration");
    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var corr = pc.getCorrelationId();
                  authz.require(pc, "account.read");
                  String accountId = mustNonEmpty(pc.getAccountId(), "account_id", corr);
                  String principalArn = requireIamPrincipalArn(corr);
                  final String externalId;
                  try {
                    externalId = awsExternalIds.getOrCreate(accountId);
                  } catch (AccountAwsExternalIdProvider.AccountMissingException missing) {
                    throw GrpcErrors.notFound(corr, ACCOUNT, Map.of("id", accountId), missing);
                  }
                  return GetAwsTrustConfigurationResponse.newBuilder()
                      .setConfiguration(
                          AwsTrustConfiguration.newBuilder()
                              .addServicePrincipalArns(principalArn)
                              .setExternalId(externalId)
                              .setRoleSessionName(
                                  AccountAwsExternalIdProvider.roleSessionName(accountId)))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private String requireIamPrincipalArn(String correlationId) {
    String configured = awsServicePrincipalArn.orElse("").trim();
    try {
      Arn arn = Arn.fromString(configured);
      String resource = arn.resourceAsString();
      boolean validResource =
          "root".equals(resource)
              || (resource.startsWith("role/") && resource.length() > "role/".length())
              || (resource.startsWith("user/") && resource.length() > "user/".length());
      if (!"iam".equals(arn.service())
          || arn.region().isPresent()
          || arn.accountId().filter(id -> id.matches("[0-9]{12}")).isEmpty()
          || !validResource) {
        throw new IllegalArgumentException("not an IAM principal ARN");
      }
      return configured;
    } catch (RuntimeException invalid) {
      throw GrpcErrors.preconditionFailed(
          correlationId, AWS_TRUST_CONFIGURATION_UNAVAILABLE, Map.of(), invalid);
    }
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
                          .setAwsExternalId(AccountAwsExternalIdProvider.newExternalId())
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

                  var existingForIdempotency = accountRepo.getByName(normName);
                  var result =
                      MutationOps.createProtoRecoverable(
                          idempotencyAccount,
                          "CreateAccount",
                          idempotencyKey,
                          () -> fingerprint,
                          () ->
                              existingForIdempotency
                                  .filter(
                                      existing ->
                                          Arrays.equals(
                                              fingerprint,
                                              canonicalFingerprint(specFromAccount(existing))))
                                  .map(Account::getResourceId)
                                  .orElse(resourceId),
                          (reservedId, committer) -> {
                            var existingOpt = accountRepo.getByName(normName);
                            if (existingOpt.isPresent()) {
                              var existing = existingOpt.get();
                              if (!existing.getResourceId().equals(reservedId)
                                  || !Arrays.equals(
                                      fingerprint,
                                      canonicalFingerprint(specFromAccount(existing)))) {
                                throw GrpcErrors.alreadyExists(
                                    corr, ACCOUNT_ALREADY_EXISTS, Map.of("display_name", normName));
                              }
                              var currentMeta = accountRepo.metaFor(reservedId);
                              var completed =
                                  accountRepo.completeWithMetaIfUnchanged(
                                      existing,
                                      currentMeta.getPointerVersion(),
                                      resource ->
                                          committer.prepareSuccessOps(
                                              new IdempotencyGuard.CommittedCreate<>(
                                                  resource.value(), reservedId, resource.meta())));
                              if (completed.isEmpty()) {
                                throw new BaseResourceRepository.AbortRetryableException(
                                    "account changed while committing idempotency receipt");
                              }
                              return new IdempotencyGuard.CommittedCreate<>(
                                  existing, reservedId, completed.get());
                            }
                            var reserved =
                                desiredAccount.toBuilder().setResourceId(reservedId).build();
                            final GenericResourceRepository.ResourceWithMeta<Account> committed;
                            try {
                              committed =
                                  accountRepo.createWithCompletion(
                                      reserved,
                                      resource ->
                                          committer.prepareSuccessOps(
                                              new IdempotencyGuard.CommittedCreate<>(
                                                  resource.value(), reservedId, resource.meta())));
                            } catch (BaseResourceRepository.NameConflictException conflict) {
                              throw GrpcErrors.alreadyExists(
                                  corr, ACCOUNT_ALREADY_EXISTS, Map.of("display_name", normName));
                            }
                            return new IdempotencyGuard.CommittedCreate<>(
                                committed.value(), reservedId, committed.meta());
                          },
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Account::parseFrom);

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

                  var deletionMeta = loadAccountDeletionFence(accountId.getId());
                  MutationMeta meta;
                  try {
                    meta = accountRepo.metaFor(accountId);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = deletionMeta.orElseGet(() -> accountRepo.metaForSafe(accountId));
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && deletionMeta.isEmpty()) {
                      throw GrpcErrors.notFound(corr, ACCOUNT, Map.of("id", accountId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, safe, request.getPrecondition());
                    if (deletionMeta.isEmpty()) {
                      // Idempotent delete of an id that never existed must not create a permanent
                      // tombstone. Every delete committed by this protocol leaves a durable fence,
                      // so an absent account without one has no cleanup to resume.
                      return DeleteAccountResponse.newBuilder().setMeta(safe).build();
                    }
                    credentialCleanup.revoke(accountId);
                    return DeleteAccountResponse.newBuilder().setMeta(safe).build();
                  }

                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());
                  MutationMeta fencedMeta = ensureAccountDeletionFence(accountId.getId(), meta);
                  if (!accountRepo.deleteWithPrecondition(
                      accountId, fencedMeta.getPointerVersion())) {
                    var current = accountRepo.metaForSafe(accountId);
                    if (current.getPointerVersion() == 0L) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "account deletion raced another delete");
                    }
                    if (current.getPointerVersion() == fencedMeta.getPointerVersion()) {
                      // DynamoDB may report TransactionConflict before the competing deletion
                      // commits. Keep the shared fence while this version can still be deleted by
                      // an in-flight attempt; clearing it would reopen descendant creates.
                      throw new BaseResourceRepository.AbortRetryableException(
                          "account deletion transaction conflicted");
                    }
                    // The account moved before the fence became effective. Preserve continuous
                    // exclusion while rebinding the fence to the new version; deleting and
                    // recreating the marker would introduce an ABA window for concurrent deleters.
                    try {
                      MutationOps.BaseServiceChecks.enforcePreconditions(
                          corr, current, request.getPrecondition());
                    } catch (RuntimeException failedPrecondition) {
                      // This invocation cannot continue. Release only the stale fence it observed.
                      clearAccountDeletionFence(accountId.getId(), fencedMeta);
                      throw failedPrecondition;
                    }
                    advanceAccountDeletionFence(accountId.getId(), fencedMeta, current);
                    throw new BaseResourceRepository.AbortRetryableException(
                        "account changed while deletion fence was installed");
                  }
                  credentialCleanup.revoke(accountId);
                  return DeleteAccountResponse.newBuilder().setMeta(fencedMeta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private java.util.Optional<MutationMeta> loadAccountDeletionFence(String accountId) {
    return pointerStore.get(Keys.accountDeletionMarker(accountId)).map(this::decodeDeletionMeta);
  }

  private MutationMeta ensureAccountDeletionFence(String accountId, MutationMeta meta) {
    String key = Keys.accountDeletionMarker(accountId);
    String payload = Base64.getEncoder().encodeToString(meta.toByteArray());
    Pointer marker = PointerReferences.opaqueMarkerPointer(key, payload, 1L);
    List<String> shardKeys = Keys.accountDeletionFenceShards(accountId);
    for (int attempt = 0; attempt < 8; attempt++) {
      var existing = pointerStore.get(key);
      if (existing.isPresent()) {
        return decodeDeletionMeta(existing.get());
      }

      List<PointerStore.CasOp> fenceCreates = new ArrayList<>(shardKeys.size() + 2);
      fenceCreates.add(new PointerStore.CasUpsert(key, 0L, marker));
      String purgeKey = Keys.accountPurgePointer(accountId);
      fenceCreates.add(
          new PointerStore.CasUpsert(
              purgeKey, 0L, PointerReferences.opaqueMarkerPointer(purgeKey, accountId, 1L)));
      for (String shardKey : shardKeys) {
        fenceCreates.add(
            new PointerStore.CasUpsert(
                shardKey, 0L, PointerReferences.opaqueMarkerPointer(shardKey, "deleting", 1L)));
      }
      if (pointerStore.compareAndSetBatch(fenceCreates)) return meta;
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "account deletion fence could not be established");
  }

  private MutationMeta decodeDeletionMeta(Pointer marker) {
    try {
      return MutationMeta.parseFrom(Base64.getDecoder().decode(marker.getBlobUri()));
    } catch (Exception e) {
      throw new BaseResourceRepository.CorruptionException(
          "invalid account deletion fence: " + marker.getKey(), e);
    }
  }

  private void advanceAccountDeletionFence(
      String accountId, MutationMeta expectedMeta, MutationMeta nextMeta) {
    String key = Keys.accountDeletionMarker(accountId);
    Pointer marker =
        pointerStore
            .get(key)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.AbortRetryableException(
                        "account deletion fence disappeared"));
    if (!decodeDeletionMeta(marker).equals(expectedMeta)) {
      throw new BaseResourceRepository.AbortRetryableException("account deletion fence changed");
    }
    String payload = Base64.getEncoder().encodeToString(nextMeta.toByteArray());
    Pointer next = PointerReferences.opaqueMarkerPointer(key, payload, marker.getVersion() + 1L);
    if (!pointerStore.compareAndSet(key, marker.getVersion(), next)) {
      throw new BaseResourceRepository.AbortRetryableException("account deletion fence changed");
    }
  }

  private void clearAccountDeletionFence(String accountId, MutationMeta expectedMeta) {
    String key = Keys.accountDeletionMarker(accountId);
    Pointer marker = pointerStore.get(key).orElse(null);
    if (marker == null) return;
    if (!decodeDeletionMeta(marker).equals(expectedMeta)) return;
    List<String> shardKeys = Keys.accountDeletionFenceShards(accountId);
    Map<String, Pointer> shards = pointerStore.getBatch(shardKeys);
    if (shards.size() != shardKeys.size()) {
      throw new BaseResourceRepository.CorruptionException(
          "incomplete account deletion fence: " + key);
    }
    List<PointerStore.CasOp> fenceDeletes = new ArrayList<>(shardKeys.size() + 2);
    fenceDeletes.add(new PointerStore.CasDelete(key, marker.getVersion()));
    String purgeKey = Keys.accountPurgePointer(accountId);
    Pointer purge = pointerStore.get(purgeKey).orElse(null);
    if (purge != null) fenceDeletes.add(new PointerStore.CasDelete(purgeKey, purge.getVersion()));
    for (String shardKey : shardKeys) {
      fenceDeletes.add(new PointerStore.CasDelete(shardKey, shards.get(shardKey).getVersion()));
    }
    if (!pointerStore.compareAndSetBatch(fenceDeletes)) {
      throw new BaseResourceRepository.AbortRetryableException(
          "account deletion fence changed while clearing");
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
