/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTraversalFailures;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.connector.spi.LogSafeText;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheck;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheckType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationIssue;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/** Read-only discovery and validation against one persisted Catalog Integration. */
@ApplicationScoped
public class CatalogIntegrationDiscovery {
  private static final Logger LOG = Logger.getLogger(CatalogIntegrationDiscovery.class);
  private static final int MAX_VALIDATION_NAMESPACES = 100_000;

  /**
   * How many tables validation will ask to vend before giving up on finding one that does.
   *
   * <p>Far smaller than the namespace ceiling because the costs differ in kind: a namespace is one
   * listing, a vend attempt is a table load plus a credential mint against the upstream. A catalog
   * where nothing vends -- an all-Azure or all-GCP workspace, where every table returns no AWS
   * credentials -- would otherwise walk the entire inventory at that price until the budget
   * expired, and report a timeout in place of the answer it already had after the first few.
   *
   * <p>Sized against that budget rather than picked round. These attempts share {@code
   * DEFAULT_VALIDATION_TIMEOUT} with the namespace walk and the storage check, and each is two
   * upstream round trips -- a table load and a real STS mint. At twenty-five that was fifty calls
   * inside thirty seconds, so a real workspace exhausted the budget first and reported a timeout
   * instead of the skipped-vend reason the loop had already accumulated.
   *
   * <p>Eight is sixteen catalog round trips -- two an attempt, a table load and a real mint -- and
   * that is not the whole cost: the storage probe moved inside this loop, so an attempt that vends
   * also runs {@code validateStorageAccess} on the same budget, which for Unity is a client
   * construction, a bounded listing and a ranged read. Two an attempt only holds because the
   * provider hands the probe the table the vend just loaded rather than loading it again; without
   * that it is three, and the arithmetic behind this number is off by half. The ceiling is
   * therefore around sixteen catalog requests and sixteen object-store requests, still inside the
   * same thirty seconds -- and it is a ceiling, not the expected path, since the first table that
   * vends and reads ends the search.
   *
   * <p>A ceiling, not the whole rule: {@link #MAX_VEND_ATTEMPTS_PER_NAMESPACE} decides how much of
   * it any one namespace may spend, so this number bounds the cost while that one bounds who gets
   * to answer.
   */
  private static final int MAX_VALIDATION_VEND_ATTEMPTS = 8;

  /**
   * How many tables validation will ask to vend within one namespace.
   *
   * <p>Because the total on its own is not enough. Checked against the whole walk, a cap of five
   * meant the first five tables in the catalog decided validation -- so an alphabetically-first
   * schema with five browsable-but-not-vendable tables reported the integration invalid even though
   * a later schema vended. That is the same ordering dependence that made a Unity PERMISSION_DENIED
   * skippable in the first place, reappearing one level up: Unity grants EXTERNAL USE SCHEMA per
   * schema, so a per-schema budget is what lets the walk sample more than one.
   *
   * <p>Two per namespace against eight in total samples at least four schemas, and sixteen upstream
   * round trips still leaves the shared validation budget close to two seconds a call. Four schemas
   * is the floor only because {@link #MAX_VEND_ATTEMPTS_PER_CATALOG} steps aside in a
   * single-catalog workspace; while it applied there unconditionally this promised four and
   * delivered two.
   */
  private static final int MAX_VEND_ATTEMPTS_PER_NAMESPACE = 2;

  /**
   * How many tables validation will ask to vend within one top-level namespace.
   *
   * <p>Because per-namespace budgeting alone still let one catalog answer for the workspace. The
   * walk is breadth-first and a Unity catalog is a one-segment namespace whose table listing is
   * empty without an RPC, so every catalog is enqueued before any schema is visited -- and the
   * first catalog popped then spent the whole total across its own schemas. On a Databricks
   * workspace that catalog is {@code hive_metastore}, which sorts ahead of {@code main}: its tables
   * report DELTA but cannot mint credentials, so all attempts failed there and the workspace was
   * reported invalid while its Unity-managed catalogs would have vended.
   *
   * <p>Four against a total of eight samples at least two catalogs without spending more upstream
   * calls than before.
   *
   * <p>Applied only where there is more than one top-level namespace. With a single catalog there
   * is nothing for it to protect -- the per-namespace budget already spreads attempts across
   * schemas -- and it instead capped the walk at two schemas, so the ordering dependence the
   * per-namespace budget removed reappeared one level up. That is the common shape: Unity OSS ships
   * a single {@code unity} catalog, and plenty of Databricks deployments use one UC catalog with
   * many schemas.
   */
  private static final int MAX_VEND_ATTEMPTS_PER_CATALOG = 4;

  /**
   * How much of a provider's failure text a validation check will repeat.
   *
   * <p>Generous enough for the refusals this codebase writes, which name a table and a reason, and
   * short enough that an upstream value cannot decide the size of a response.
   */
  private static final int MAX_SUMMARY_CHARS = 512;

  private static final Duration DEFAULT_VALIDATION_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration DEFAULT_LIST_TIMEOUT = Duration.ofSeconds(30);

  @Inject CatalogIntegrationAccess access;
  Clock clock = Clock.systemUTC();
  Duration validationTimeout = DEFAULT_VALIDATION_TIMEOUT;
  Duration listTimeout = DEFAULT_LIST_TIMEOUT;
  LongSupplier nanoTime = System::nanoTime;

  ValidationResult validate(CatalogIntegration integration) {
    CatalogUpstreamBudget budget = CatalogUpstreamBudget.start(validationTimeout, nanoTime);
    CatalogClient client;
    try {
      client = open(integration, budget);
    } catch (CatalogAccessException failure) {
      if (failure.code() == CatalogAccessException.Code.INTERNAL) {
        throw failure;
      }
      return clientOpenFailure(failure);
    }
    try {
      return validate(client, budget);
    } finally {
      closeClient(client);
    }
  }

  private ValidationResult validate(CatalogClient client, CatalogUpstreamBudget budget) {
    List<CatalogIntegrationValidationCheck> checks = new ArrayList<>();
    CatalogCapabilities capabilities = client.capabilities();
    if (!capabilities.supports(CatalogCapability.VALIDATE)) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
              CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
              "The catalog provider does not support connection validation."));
      checks.add(
          notRun(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
              "Authentication was not tested because connection validation is unsupported."));
      addNotRunAfterAuthentication(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    try {
      budget.run(client::validate);
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
              "The catalog endpoint is reachable."));
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
              "The catalog endpoint accepted the configured authentication."));
    } catch (CatalogAccessException failure) {
      if (isAuthenticationFailure(failure)) {
        checks.add(
            passed(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
                "The catalog endpoint is reachable."));
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
                CatalogIntegrationValidationIssue.CIVI_AUTHENTICATION_FAILED,
                safeSummary("Catalog authentication failed.", failure)));
        addNotRunAfterAuthentication(checks);
      } else {
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
                CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
                safeSummary("Catalog connection validation failed.", failure)));
        checks.add(
            notRun(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
                "Authentication was not tested because the catalog endpoint was unavailable."));
        addNotRunAfterAuthentication(checks);
      }
      return new ValidationResult(false, checks, capabilities);
    }

    if (!capabilities.supports(CatalogCapability.LIST_NAMESPACES)
        || !capabilities.supports(CatalogCapability.LIST_TABLES)) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              CatalogIntegrationValidationIssue.CIVI_DISCOVERY_UNSUPPORTED,
              "The catalog provider cannot enumerate namespaces and tables."));
      addNotRunAfterDiscovery(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    boolean canVend = capabilities.supports(CatalogCapability.VEND_STORAGE_CREDENTIALS);
    boolean canValidateStorage = capabilities.supports(CatalogCapability.VALIDATE_STORAGE_ACCESS);
    ValidationTableSearch validationTables;
    try {
      validationTables =
          findValidationTable(
              client, budget, canVend, canVend && canValidateStorage, clock.instant());
    } catch (SamplingTimeout timeout) {
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              "The catalog can enumerate upstream namespaces and tables."));
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED,
              safeSummary(
                  "Storage credential vending did not finish within the validation budget.",
                  timeout.failure())));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    } catch (StorageAccessFailure failure) {
      // Vending is reported passed here on purpose: the search reached a storage probe, which it
      // only does with a live credential in hand.
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              "The catalog can enumerate upstream namespaces and tables."));
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              "The catalog vended scoped storage credentials."));
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
              credentialIssue(
                  failure.failure(), CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_FAILED),
              safeSummary("Storage access validation failed.", failure.failure())));
      return new ValidationResult(false, checks, capabilities);
    } catch (CredentialVendingFailure failure) {
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              "The catalog can enumerate upstream namespaces and tables."));
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              credentialIssue(
                  failure.failure(),
                  CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED),
              safeSummary("Storage credential vending failed.", failure.failure())));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    } catch (CatalogAccessException failure) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              CatalogIntegrationValidationIssue.CIVI_DISCOVERY_FAILED,
              safeSummary("Catalog discovery validation failed.", failure)));
      addNotRunAfterDiscovery(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    if (!validationTables.foundTable()) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              CatalogIntegrationValidationIssue.CIVI_NO_TABLES,
              "No upstream table is available to validate credential vending."));
      addNotRunAfterDiscovery(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    checks.add(
        passed(
            CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
            "The catalog can enumerate upstream namespaces and tables."));

    if (!canVend) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_UNSUPPORTED,
              "The catalog provider does not support storage credential vending."));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    if (validationTables.storageFailure().isPresent()) {
      // Every sampled table that vended was then refused its own storage read. Vending passed --
      // it demonstrably worked -- and the storage step reports the first refusal stepped over,
      // rather than the whole integration being decided by whichever table sorted first.
      CatalogAccessException failure = validationTables.storageFailure().orElseThrow();
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              "The catalog vended scoped storage credentials."));
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
              credentialIssue(
                  failure, CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_FAILED),
              safeSummary("Storage access validation failed.", failure)));
      return new ValidationResult(false, checks, capabilities);
    }
    if (validationTables.target().isEmpty()) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED,
              "The catalog did not vend usable storage credentials for an upstream table."));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    ValidationTarget target = validationTables.target().orElseThrow();
    CatalogObjectName table = target.table();
    VendedStorageCredentials vendedCredentials = target.credentials().orElseThrow();
    if (vendedCredentials
        .expiresAt()
        .filter(expiry -> !expiry.isAfter(clock.instant()))
        .isPresent()) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_EXPIRED,
              "The catalog vended storage credentials that are already expired."));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    checks.add(
        passed(
            CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
            "The catalog vended scoped storage credentials."));

    if (!canValidateStorage) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
              CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_UNSUPPORTED,
              "The catalog provider cannot validate access to upstream table storage."));
      return new ValidationResult(false, checks, capabilities);
    }
    // Already proven inside the search, on this table, with these credentials. The probe moved
    // there so a table that vends and then cannot be read is stepped over like a table that will
    // not vend; re-running it here would double every read and could disagree with itself.
    if (!target.storageValidated()) {
      throw new IllegalStateException(
          "Validation target was not storage-checked while the provider supports it: " + table);
    }
    checks.add(
        passed(
            CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
            "Vended credentials can read upstream table storage."));
    return new ValidationResult(true, checks, capabilities);
  }

  List<NamespacePath> listNamespaces(CatalogIntegration integration, NamespacePath parent) {
    CatalogUpstreamBudget budget = CatalogUpstreamBudget.start(listTimeout, nanoTime);
    try (CatalogClient client = open(integration, budget)) {
      require(client.capabilities(), CatalogCapability.LIST_NAMESPACES);
      return budget.call(() -> client.listNamespaces(parent)).stream().distinct().sorted().toList();
    }
  }

  List<DiscoveredObject> listObjects(
      CatalogIntegration integration, NamespacePath namespace, Set<ObjectKind> requestedKinds) {
    CatalogUpstreamBudget budget = CatalogUpstreamBudget.start(listTimeout, nanoTime);
    try (CatalogClient client = open(integration, budget)) {
      CatalogCapabilities capabilities = client.capabilities();
      Set<ObjectKind> kinds =
          requestedKinds.isEmpty() ? defaultKinds(capabilities) : EnumSet.copyOf(requestedKinds);
      List<DiscoveredObject> objects = new ArrayList<>();
      if (kinds.contains(ObjectKind.TABLE)) {
        require(capabilities, CatalogCapability.LIST_TABLES);
        budget.call(() -> client.listTables(namespace)).stream()
            .map(name -> discovered(name, namespace, ObjectKind.TABLE))
            .forEach(objects::add);
      }
      if (kinds.contains(ObjectKind.VIEW)) {
        require(capabilities, CatalogCapability.LIST_VIEWS);
        budget.call(() -> client.listViews(namespace)).stream()
            .map(name -> discovered(name, namespace, ObjectKind.VIEW))
            .forEach(objects::add);
      }
      return objects.stream().distinct().sorted().toList();
    }
  }

  private static ValidationTableSearch findValidationTable(
      CatalogClient client,
      CatalogUpstreamBudget budget,
      boolean vendCredentials,
      boolean validateStorage,
      Instant now) {
    Set<CatalogObjectName> seenTables = new HashSet<>();
    // The first per-table storage-access failure the search stepped over, kept for the same reason
    // as the vending one beside it. A recorded value also means some table vended, so the caller
    // reports vending as passed and this as the storage failure rather than blaming the vend.
    var skippedStorageFailure =
        new java.util.concurrent.atomic.AtomicReference<CatalogAccessException>();
    // The first per-table vending failure the search stepped over. Kept so that a search which
    // finds nothing can report why rather than the blanker "nothing vended": every table skipped
    // for the same reason is a diagnosis, and losing it would be the cost of skipping at all.
    var skippedVendFailure =
        new java.util.concurrent.atomic.AtomicReference<CatalogAccessException>();
    // Set once the vend-attempt cap trips, which ends the whole search rather than one batch.
    var exhausted = new java.util.concurrent.atomic.AtomicBoolean();
    var vendAttempts = new java.util.concurrent.atomic.AtomicInteger();
    // Shared across namespaces, keyed on the top-level segment: one catalog must not answer for
    // the workspace just because the breadth-first walk reaches its schemas first.
    var catalogAttempts = new java.util.HashMap<String, Integer>();
    // How many catalogs the workspace has, known once the root listing returns. The per-catalog cap
    // is meaningless below two and actively harmful at one, so it is gated on this.
    var topLevelCount = new java.util.concurrent.atomic.AtomicInteger();
    // One instant for the whole walk, taken by the caller: comparing each tuple against a fresh
    // now would let a credential expire mid-search and change which table answers.
    List<CatalogObjectName> rootTables;
    try {
      rootTables = budget.call(() -> client.listTables(NamespacePath.root()));
    } catch (CatalogAccessException failure) {
      rootTables = namespaceTableListingSkippable(failure) ? List.of() : throwFailure(failure);
    }
    Optional<ValidationTarget> target =
        samplingTarget(
            rootTables,
            seenTables,
            client,
            budget,
            vendCredentials,
            validateStorage,
            skippedVendFailure,
            skippedStorageFailure,
            exhausted,
            vendAttempts,
            catalogAttempts,
            topLevelCount,
            now);
    boolean foundTable = !seenTables.isEmpty();
    if (target.isPresent()) {
      return new ValidationTableSearch(true, target, Optional.empty());
    }
    if (exhausted.get()) {
      return exhaustedSearch(foundTable, skippedVendFailure, skippedStorageFailure);
    }
    Set<NamespacePath> seen = new HashSet<>();
    var pending = new ArrayDeque<NamespacePath>();
    pending.add(NamespacePath.root());
    while (!pending.isEmpty()) {
      NamespacePath parent = pending.removeFirst();
      List<NamespacePath> children;
      try {
        children = budget.call(() -> client.listNamespaces(parent));
      } catch (CatalogAccessException failure) {
        // The same guard both listTables calls already carry. A Unity workspace almost always
        // exposes a system catalog whose schemas the integration principal cannot list, and this
        // search no longer stops at the first table it finds -- it keeps going until one vends --
        // so a 403 on a single parent that used to be unreachable now aborts the whole validation.
        // The root listing is not a branch, it is the whole tree -- the same carve-out
        // CatalogOverlayReconciler.tolerateBranchFailure makes, and the shared helper leaves this
        // judgement to its callers because it turns on the path rather than the code. Tolerating a
        // root denial drains `pending` at once and reports "no upstream table is available", which
        // describes the workspace instead of the lost permission that actually stopped the walk --
        // and an integration that validates on a root its principal cannot enumerate then fails
        // reconcile on the very same call.
        if (parent.segments().isEmpty() || !namespaceTableListingSkippable(failure)) {
          throwFailure(failure);
        }
        continue;
      }
      if (parent.segments().isEmpty()) {
        // The catalogs, all of them, before any schema is visited -- the walk is breadth-first and
        // the root is popped first. This is what decides whether the per-catalog cap applies at
        // all.
        topLevelCount.set(children.size());
      }
      for (NamespacePath namespace : children.stream().sorted().toList()) {
        budget.check();
        if (!seen.add(namespace)) {
          continue;
        }
        if (seen.size() > MAX_VALIDATION_NAMESPACES) {
          throw new CatalogAccessException(
              CatalogAccessException.Code.UNAVAILABLE,
              "Catalog namespace inventory exceeds the validation limit");
        }
        // Pruned once this catalog's vend budget is spent. Returning from the table batch alone
        // stopped the attempts but not the walk: the traversal kept listing every remaining schema
        // under the same catalog and enqueueing their descendants, and in a single-catalog
        // workspace the per-catalog cap trips before the total one, so `exhausted` never fires. A
        // large catalog whose sampled tables cannot vend then spent the whole budget enumerating
        // schemas and reported a discovery timeout instead of the bounded vending failure these
        // caps exist to produce. Sibling catalogs are already enqueued from the root walk, so they
        // are still sampled.
        if (vendCredentials && catalogSpent(namespace, catalogAttempts, topLevelCount)) {
          continue;
        }
        List<CatalogObjectName> tables;
        try {
          tables = budget.call(() -> client.listTables(namespace));
        } catch (CatalogAccessException failure) {
          tables = namespaceTableListingSkippable(failure) ? List.of() : throwFailure(failure);
        }
        target =
            samplingTarget(
                tables,
                seenTables,
                client,
                budget,
                vendCredentials,
                validateStorage,
                skippedVendFailure,
                skippedStorageFailure,
                exhausted,
                vendAttempts,
                catalogAttempts,
                topLevelCount,
                now);
        foundTable |= !tables.isEmpty();
        if (target.isPresent()) {
          return new ValidationTableSearch(true, target, Optional.empty());
        }
        if (exhausted.get()) {
          return exhaustedSearch(foundTable, skippedVendFailure, skippedStorageFailure);
        }
        pending.addLast(namespace);
      }
    }
    return exhaustedSearch(foundTable, skippedVendFailure, skippedStorageFailure);
  }

  /**
   * The answer for a search that ran out of tables, attempts, or namespaces without one vending.
   *
   * <p>Reports the first failure it stepped over when there was one: every table skipped for the
   * same reason is the diagnosis, and dropping it for a blanker "nothing vended" is what made
   * skipping cost something.
   */
  private static ValidationTableSearch exhaustedSearch(
      boolean foundTable,
      java.util.concurrent.atomic.AtomicReference<CatalogAccessException> skippedVendFailure,
      java.util.concurrent.atomic.AtomicReference<CatalogAccessException> skippedStorageFailure) {
    // Checked first: a storage failure can only have been recorded after a table vended, so
    // reporting the vending failure from some earlier table would describe the integration as
    // unable to vend when it demonstrably can.
    if (skippedStorageFailure.get() != null) {
      return new ValidationTableSearch(
          foundTable, Optional.empty(), Optional.of(skippedStorageFailure.get()));
    }
    if (skippedVendFailure.get() != null) {
      throw new CredentialVendingFailure(skippedVendFailure.get());
    }
    return new ValidationTableSearch(foundTable, Optional.empty(), Optional.empty());
  }

  /**
   * {@link #findValidationTarget} with a budget expiry named for what it interrupted.
   *
   * <p>The per-table {@code budget.check()} sits outside the try that classifies a vending failure,
   * so a timeout during sampling unwound into the caller's generic handler and reported
   * CIVI_DISCOVERY_FAILED with vending and storage marked not run -- while discovery had
   * demonstrably succeeded and vending was underway. That points an operator at their namespace
   * listing rather than at the vend that was actually slow.
   *
   * <p>Sound only because of where this sits: the search reaches a table batch only after listing
   * produced one, so a TIMEOUT raised inside is post-discovery by construction. A timeout in the
   * listing walk itself is still a discovery failure and still reported as one.
   */
  private static Optional<ValidationTarget> samplingTarget(
      List<CatalogObjectName> tables,
      Set<CatalogObjectName> seenTables,
      CatalogClient client,
      CatalogUpstreamBudget budget,
      boolean vendCredentials,
      boolean validateStorage,
      java.util.concurrent.atomic.AtomicReference<CatalogAccessException> skippedVendFailure,
      java.util.concurrent.atomic.AtomicReference<CatalogAccessException> skippedStorageFailure,
      java.util.concurrent.atomic.AtomicBoolean exhausted,
      java.util.concurrent.atomic.AtomicInteger vendAttempts,
      java.util.Map<String, Integer> catalogAttempts,
      java.util.concurrent.atomic.AtomicInteger topLevelCount,
      Instant now) {
    try {
      return findValidationTarget(
          tables,
          seenTables,
          client,
          budget,
          vendCredentials,
          validateStorage,
          skippedVendFailure,
          skippedStorageFailure,
          exhausted,
          vendAttempts,
          catalogAttempts,
          topLevelCount,
          now);
    } catch (CatalogAccessException failure) {
      if (failure.code() == CatalogAccessException.Code.TIMEOUT) {
        throw new SamplingTimeout(failure);
      }
      throw failure;
    }
  }

  private static Optional<ValidationTarget> findValidationTarget(
      List<CatalogObjectName> tables,
      Set<CatalogObjectName> seenTables,
      CatalogClient client,
      CatalogUpstreamBudget budget,
      boolean vendCredentials,
      boolean validateStorage,
      java.util.concurrent.atomic.AtomicReference<CatalogAccessException> skippedVendFailure,
      java.util.concurrent.atomic.AtomicReference<CatalogAccessException> skippedStorageFailure,
      java.util.concurrent.atomic.AtomicBoolean exhausted,
      java.util.concurrent.atomic.AtomicInteger vendAttempts,
      java.util.Map<String, Integer> catalogAttempts,
      java.util.concurrent.atomic.AtomicInteger topLevelCount,
      Instant now) {
    // Per namespace, so it resets on each call; the total is shared across them.
    int[] namespaceAttempts = {0};
    for (CatalogObjectName table : tables.stream().sorted().toList()) {
      budget.check();
      if (!seenTables.add(table)) {
        continue;
      }
      if (!vendCredentials) {
        return Optional.of(new ValidationTarget(table, Optional.empty(), false));
      }
      String catalog =
          table.namespace().segments().isEmpty() ? "" : table.namespace().segments().getFirst();
      if (perCatalogCapApplies(topLevelCount)
          && catalogAttempts.getOrDefault(catalog, 0) >= MAX_VEND_ATTEMPTS_PER_CATALOG) {
        // This catalog has had its share; leave without exhausting so the walk reaches the next.
        return Optional.empty();
      }
      if (namespaceAttempts[0] >= MAX_VEND_ATTEMPTS_PER_NAMESPACE) {
        // This namespace has had its share. Leave without setting exhausted so the walk moves to
        // the next one, which is the whole point: one unlucky schema must not decide validation.
        return Optional.empty();
      }
      if (vendAttempts.get() >= MAX_VALIDATION_VEND_ATTEMPTS) {
        // Signalled, not just returned. An empty answer means "no vendable table here" and the
        // caller walks on to the next namespace, which is how the first version of this cap left
        // the traversal running -- listing the whole tree at two RPCs a namespace until the budget
        // expired, and reporting that timeout instead of the answer it had after the first few.
        exhausted.set(true);
        return Optional.empty();
      }
      namespaceAttempts[0]++;
      catalogAttempts.merge(catalog, 1, Integer::sum);
      vendAttempts.incrementAndGet();
      try {
        Optional<VendedStorageCredentials> credentials =
            budget.call(() -> client.vendStorageCredentials(table));
        if (credentials.isPresent()) {
          // Expiry decided here, not by the caller. Returning the first non-empty tuple and
          // failing validation on its expiry outside the loop meant an already-expired credential
          // from the first table ended the search, while the next table would have vended a live
          // one -- the table-order dependence this bounded search exists to remove. It is also the
          // same condition vendFailureSkippable already treats as per-table when a provider throws
          // CREDENTIAL_EXPIRED, so arriving as a returned value should not be terminal instead.
          if (isExpired(credentials.get(), now)) {
            skippedVendFailure.compareAndSet(
                null,
                new CatalogAccessException(
                    CatalogAccessException.Code.CREDENTIAL_EXPIRED,
                    "Catalog vended already-expired storage credentials for " + table));
            continue;
          }
          if (validateStorage) {
            VendedStorageCredentials vended = credentials.get();
            try {
              budget.run(() -> client.validateStorageAccess(table, vended));
            } catch (CatalogAccessException failure) {
              if (!storageFailureSkippable(failure)) {
                throw new StorageAccessFailure(failure);
              }
              skippedStorageFailure.compareAndSet(null, failure);
              continue;
            }
          }
          return Optional.of(new ValidationTarget(table, credentials, validateStorage));
        }
      } catch (CatalogAccessException failure) {
        if (!vendFailureSkippable(failure)) {
          throw new CredentialVendingFailure(failure);
        }
        skippedVendFailure.compareAndSet(null, failure);
      }
    }
    return Optional.empty();
  }

  /**
   * Whether this namespace's top-level catalog has already used its share of vend attempts.
   *
   * <p>Derived rather than tracked separately: the counter the gate in {@code findValidationTarget}
   * reads is the same one, so the two cannot disagree about which catalog is finished.
   */
  private static boolean perCatalogCapApplies(
      java.util.concurrent.atomic.AtomicInteger topLevelCount) {
    return topLevelCount.get() > 1;
  }

  private static boolean catalogSpent(
      NamespacePath namespace,
      java.util.Map<String, Integer> catalogAttempts,
      java.util.concurrent.atomic.AtomicInteger topLevelCount) {
    if (!perCatalogCapApplies(topLevelCount) || namespace.segments().isEmpty()) {
      return false;
    }
    return catalogAttempts.getOrDefault(namespace.segments().getFirst(), 0)
        >= MAX_VEND_ATTEMPTS_PER_CATALOG;
  }

  /** Whether a vended tuple has already lapsed, and so cannot demonstrate anything about access. */
  private static boolean isExpired(VendedStorageCredentials credentials, Instant now) {
    return credentials.expiresAt().filter(expiry -> !expiry.isAfter(now)).isPresent();
  }

  /**
   * Whether a storage-access failure describes this table rather than the integration.
   *
   * <p>The vending set plus {@code INVALID_CONFIGURATION}, which for a read is a fact about one
   * table's location: a bucket outside the integration's configured region answers
   * PermanentRedirect, and the Unity validator maps that here. Over-including costs at most a few
   * more probes, because validation only passes when some table demonstrably reads; under-including
   * fails a healthy integration on whichever table happened to sort first.
   *
   * <p>What stays terminal is what will answer the same for every table: an unreachable store, an
   * internal fault. Walking the sample to collect one repeated failure buys nothing.
   */
  private static boolean storageFailureSkippable(CatalogAccessException failure) {
    return switch (failure.code()) {
      case INVALID_CONFIGURATION -> true;
      default -> vendFailureSkippable(failure);
    };
  }

  /**
   * Whether a vending failure describes this table rather than the integration.
   *
   * <p>The search exists to find a table that vends, and an empty answer already moves on to the
   * next one; a failure that means the same thing has to move on too, or the alphabetically first
   * table decides validation for a catalog whose second table would have vended fine. A table can
   * be dropped between listing and vending, expose no stable table ID, sit outside the scope the
   * catalog vends for, or come back with credentials the provider cannot assemble -- none of which
   * says anything about the next table.
   *
   * <p>Everything else stops the search where it stands. An authorization refusal, a configuration
   * that cannot serve the request, an upstream that is unreachable: each will answer identically
   * for every remaining table, so walking the inventory to collect the same failure only spends the
   * budget and hammers the upstream to arrive at a worse-reported version of what is already known.
   */
  private static boolean vendFailureSkippable(CatalogAccessException failure) {
    return switch (failure.code()) {
      case NOT_FOUND, UNSUPPORTED, CREDENTIAL_SCOPE_INVALID, CREDENTIAL_EXPIRED -> true;
      // PERMISSION_DENIED too, because a vend grant is not integration-wide. Unity gates
      // generateTemporaryTableCredentials on EXTERNAL USE SCHEMA, granted per schema, so a
      // principal can browse a workspace and be refused a vend in some schemas and not others --
      // and whether validation passed came down to whether an ungranted schema sorted first.
      // CatalogTraversalFailures already steps over a branch the principal cannot list, so
      // treating the vend differently was the inconsistency.
      case PERMISSION_DENIED -> true;
      // Not INTERNAL. It was admitted here only to make one table-scoped Unity failure skippable,
      // and that failure now raises UNSUPPORTED with its neighbours. INTERNAL is also what
      // UnityCatalogErrors produces for INVALID_RESPONSE -- a fault of the catalog, not of a
      // table -- so tolerating it walked the inventory minting credentials to collect the same
      // answer, and losing the diagnosis entirely if the budget expired first.
      default -> false;
    };
  }

  private record ValidationTableSearch(
      boolean foundTable,
      Optional<ValidationTarget> target,
      Optional<CatalogAccessException> storageFailure) {}

  private record ValidationTarget(
      CatalogObjectName table,
      Optional<VendedStorageCredentials> credentials,
      boolean storageValidated) {}

  /** A budget expiry that interrupted vend sampling rather than discovery. */
  private static final class SamplingTimeout extends RuntimeException {
    private final CatalogAccessException failure;

    private SamplingTimeout(CatalogAccessException failure) {
      super(failure);
      this.failure = failure;
    }

    private CatalogAccessException failure() {
      return failure;
    }
  }

  /** A storage-access failure that describes the integration, so the search stops at it. */
  private static final class StorageAccessFailure extends RuntimeException {
    private final CatalogAccessException failure;

    private StorageAccessFailure(CatalogAccessException failure) {
      super(failure);
      this.failure = failure;
    }

    private CatalogAccessException failure() {
      return failure;
    }
  }

  private static final class CredentialVendingFailure extends RuntimeException {
    private final CatalogAccessException failure;

    private CredentialVendingFailure(CatalogAccessException failure) {
      super(failure);
      this.failure = failure;
    }

    private CatalogAccessException failure() {
      return failure;
    }
  }

  /**
   * Whether a listing failure describes one namespace rather than the catalog.
   *
   * <p>Delegates so validation and {@code CatalogOverlayReconciler} cannot drift: an integration
   * that validates by skipping an inaccessible branch has to be able to reconcile past the same
   * one.
   */
  private static boolean namespaceTableListingSkippable(CatalogAccessException failure) {
    return CatalogTraversalFailures.describesOneBranch(failure);
  }

  private static <T> T throwFailure(RuntimeException failure) {
    throw failure;
  }

  private CatalogClient open(CatalogIntegration integration, CatalogUpstreamBudget budget) {
    return budget.call(() -> access.open(integration), CatalogIntegrationDiscovery::closeClient);
  }

  private static Set<ObjectKind> defaultKinds(CatalogCapabilities capabilities) {
    EnumSet<ObjectKind> kinds = EnumSet.noneOf(ObjectKind.class);
    if (capabilities.supports(CatalogCapability.LIST_TABLES)) {
      kinds.add(ObjectKind.TABLE);
    }
    if (capabilities.supports(CatalogCapability.LIST_VIEWS)) {
      kinds.add(ObjectKind.VIEW);
    }
    return kinds;
  }

  private static DiscoveredObject discovered(
      CatalogObjectName name, NamespacePath expectedNamespace, ObjectKind kind) {
    if (!name.namespace().equals(expectedNamespace)) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INTERNAL,
          "Catalog provider returned an object outside the requested namespace");
    }
    return new DiscoveredObject(name, kind);
  }

  private static void require(CatalogCapabilities capabilities, CatalogCapability capability) {
    if (!capabilities.supports(capability)) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Catalog provider does not support " + capability.name());
    }
  }

  /**
   * The reason a check failed, as a caller may safely be shown it.
   *
   * <p>Bounded and stripped, because this is upstream-controlled text on its way into a gRPC
   * response and a terminal. A provider interpolates names and declared types it was given -- this
   * one puts fully-qualified table names and raw {@code type_text} into its refusals -- so an
   * upstream value decides both the size of the message and whether it carries control characters.
   * The log path is already held to this; the response path was not, and it is the one with no
   * other bound in front of it.
   */
  private static String safeSummary(String fallback, RuntimeException failure) {
    String message =
        failure instanceof CatalogAccessException accessFailure ? accessFailure.getMessage() : null;
    if (message == null || message.isBlank()) {
      return fallback;
    }
    return LogSafeText.bounded(message, MAX_SUMMARY_CHARS);
  }

  private static ValidationResult clientOpenFailure(CatalogAccessException failure) {
    List<CatalogIntegrationValidationCheck> checks = new ArrayList<>();
    if (isAuthenticationFailure(failure)) {
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
              "The catalog endpoint is reachable."));
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
              CatalogIntegrationValidationIssue.CIVI_AUTHENTICATION_FAILED,
              safeSummary("Catalog authentication failed.", failure)));
    } else {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
              CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
              safeSummary("The catalog client could not be opened.", failure)));
      checks.add(
          notRun(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
              "Authentication was not tested because the catalog client could not be opened."));
    }
    addNotRunAfterAuthentication(checks);
    return new ValidationResult(false, checks, CatalogCapabilities.of());
  }

  private static boolean isAuthenticationFailure(CatalogAccessException failure) {
    return failure.code() == CatalogAccessException.Code.UNAUTHENTICATED
        || failure.code() == CatalogAccessException.Code.PERMISSION_DENIED;
  }

  private static CatalogIntegrationValidationIssue credentialIssue(
      CatalogAccessException failure, CatalogIntegrationValidationIssue fallback) {
    return switch (failure.code()) {
      case CREDENTIAL_EXPIRED -> CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_EXPIRED;
      case CREDENTIAL_SCOPE_INVALID ->
          CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_SCOPE_INVALID;
      default -> fallback;
    };
  }

  private static void closeClient(CatalogClient client) {
    try {
      client.close();
    } catch (RuntimeException failure) {
      LOG.warnf("Catalog client close failed: type=%s", failure.getClass().getName());
    }
  }

  private static CatalogIntegrationValidationCheck passed(
      CatalogIntegrationValidationCheckType type, String summary) {
    return CatalogIntegrationValidationCheck.newBuilder()
        .setType(type)
        .setStatus(CatalogIntegrationValidationStatus.CIVS_PASSED)
        .setSummary(summary)
        .build();
  }

  private static CatalogIntegrationValidationCheck failed(
      CatalogIntegrationValidationCheckType type,
      CatalogIntegrationValidationIssue issue,
      String summary) {
    return CatalogIntegrationValidationCheck.newBuilder()
        .setType(type)
        .setStatus(CatalogIntegrationValidationStatus.CIVS_FAILED)
        .setIssue(issue)
        .setSummary(summary)
        .build();
  }

  private static CatalogIntegrationValidationCheck notRun(
      CatalogIntegrationValidationCheckType type, String summary) {
    return CatalogIntegrationValidationCheck.newBuilder()
        .setType(type)
        .setStatus(CatalogIntegrationValidationStatus.CIVS_NOT_RUN)
        .setSummary(summary)
        .build();
  }

  private static void addNotRunAfterAuthentication(List<CatalogIntegrationValidationCheck> checks) {
    checks.add(
        notRun(
            CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
            "Discovery was not tested because connection and authentication did not pass."));
    addNotRunAfterDiscovery(checks);
  }

  private static void addNotRunAfterDiscovery(List<CatalogIntegrationValidationCheck> checks) {
    checks.add(
        notRun(
            CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
            "Credential vending was not tested because discovery did not pass."));
    addStorageNotRun(checks);
  }

  private static void addStorageNotRun(List<CatalogIntegrationValidationCheck> checks) {
    checks.add(
        notRun(
            CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
            "Storage access was not tested because credential vending did not pass."));
  }

  enum ObjectKind {
    TABLE,
    VIEW
  }

  record DiscoveredObject(CatalogObjectName name, ObjectKind kind)
      implements Comparable<DiscoveredObject> {
    @Override
    public int compareTo(DiscoveredObject other) {
      int byName = name.compareTo(other.name);
      return byName == 0 ? kind.compareTo(other.kind) : byName;
    }
  }

  record ValidationResult(
      boolean valid,
      List<CatalogIntegrationValidationCheck> checks,
      CatalogCapabilities capabilities) {
    ValidationResult {
      checks = List.copyOf(checks);
    }
  }
}
