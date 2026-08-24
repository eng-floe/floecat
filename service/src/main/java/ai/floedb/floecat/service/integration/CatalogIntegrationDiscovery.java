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
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheck;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheckType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationIssue;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus;
import ai.floedb.floecat.service.context.PropagatedContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/** Read-only discovery and validation against one persisted Catalog Integration. */
@ApplicationScoped
public class CatalogIntegrationDiscovery {
  private static final Logger LOG = Logger.getLogger(CatalogIntegrationDiscovery.class);
  private static final int MAX_VALIDATION_NAMESPACES = 100_000;
  private static final Duration DEFAULT_VALIDATION_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration DEFAULT_LIST_TIMEOUT = Duration.ofSeconds(30);

  @Inject CatalogIntegrationAccess access;
  Clock clock = Clock.systemUTC();
  Duration validationTimeout = DEFAULT_VALIDATION_TIMEOUT;
  Duration listTimeout = DEFAULT_LIST_TIMEOUT;
  LongSupplier nanoTime = System::nanoTime;

  ValidationResult validate(CatalogIntegration integration) {
    ValidationBudget budget = ValidationBudget.start(validationTimeout, nanoTime);
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

  private ValidationResult validate(CatalogClient client, ValidationBudget budget) {
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

    Optional<CatalogObjectName> validationTable;
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
    try {
      validationTable = findFirstTable(client, budget);
    } catch (CatalogAccessException failure) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
              CatalogIntegrationValidationIssue.CIVI_DISCOVERY_FAILED,
              safeSummary("Catalog discovery validation failed.", failure)));
      addNotRunAfterDiscovery(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    if (validationTable.isEmpty()) {
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

    CatalogObjectName table = validationTable.orElseThrow();
    if (!capabilities.supports(CatalogCapability.VEND_STORAGE_CREDENTIALS)) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_UNSUPPORTED,
              "The catalog provider does not support storage credential vending."));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    }
    VendedStorageCredentials vendedCredentials;
    try {
      Optional<VendedStorageCredentials> vended =
          budget.call(() -> client.vendStorageCredentials(table));
      if (vended.isEmpty()) {
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
                CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED,
                "The catalog did not vend usable storage credentials for an upstream table."));
        addStorageNotRun(checks);
        return new ValidationResult(false, checks, capabilities);
      }
      vendedCredentials = vended.orElseThrow();
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
    } catch (CatalogAccessException failure) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
              credentialIssue(
                  failure, CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED),
              safeSummary("Storage credential vending failed.", failure)));
      addStorageNotRun(checks);
      return new ValidationResult(false, checks, capabilities);
    }

    if (!capabilities.supports(CatalogCapability.VALIDATE_STORAGE_ACCESS)) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
              CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_UNSUPPORTED,
              "The catalog provider cannot validate access to upstream table storage."));
      return new ValidationResult(false, checks, capabilities);
    }
    try {
      budget.run(() -> client.validateStorageAccess(table, vendedCredentials));
      checks.add(
          passed(
              CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
              "Vended credentials can read upstream table storage."));
      return new ValidationResult(true, checks, capabilities);
    } catch (CatalogAccessException failure) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
              credentialIssue(
                  failure, CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_FAILED),
              safeSummary("Storage access validation failed.", failure)));
      return new ValidationResult(false, checks, capabilities);
    }
  }

  List<NamespacePath> listNamespaces(CatalogIntegration integration, NamespacePath parent) {
    ValidationBudget budget = ValidationBudget.start(listTimeout, nanoTime);
    try (CatalogClient client = open(integration, budget)) {
      require(client.capabilities(), CatalogCapability.LIST_NAMESPACES);
      return budget.call(() -> client.listNamespaces(parent)).stream().distinct().sorted().toList();
    }
  }

  List<DiscoveredObject> listObjects(
      CatalogIntegration integration, NamespacePath namespace, Set<ObjectKind> requestedKinds) {
    ValidationBudget budget = ValidationBudget.start(listTimeout, nanoTime);
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

  private static Optional<CatalogObjectName> findFirstTable(
      CatalogClient client, ValidationBudget budget) {
    List<CatalogObjectName> rootTables;
    try {
      rootTables = budget.call(() -> client.listTables(NamespacePath.root()));
    } catch (CatalogAccessException failure) {
      rootTables = namespaceTableListingSkippable(failure) ? List.of() : throwFailure(failure);
    }
    if (!rootTables.isEmpty()) {
      return Optional.of(rootTables.stream().sorted().findFirst().orElseThrow());
    }
    Set<NamespacePath> seen = new HashSet<>();
    var pending = new ArrayDeque<NamespacePath>();
    pending.add(NamespacePath.root());
    while (!pending.isEmpty()) {
      NamespacePath parent = pending.removeFirst();
      List<NamespacePath> children = budget.call(() -> client.listNamespaces(parent));
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
        List<CatalogObjectName> tables;
        try {
          tables = budget.call(() -> client.listTables(namespace));
        } catch (CatalogAccessException failure) {
          tables = namespaceTableListingSkippable(failure) ? List.of() : throwFailure(failure);
        }
        budget.check();
        if (!tables.isEmpty()) {
          return Optional.of(tables.stream().sorted().findFirst().orElseThrow());
        }
        pending.addLast(namespace);
      }
    }
    return Optional.empty();
  }

  private static boolean namespaceTableListingSkippable(CatalogAccessException failure) {
    return switch (failure.code()) {
      case INVALID_CONFIGURATION, NOT_FOUND, PERMISSION_DENIED, UNSUPPORTED -> true;
      default -> false;
    };
  }

  private static <T> T throwFailure(RuntimeException failure) {
    throw failure;
  }

  private CatalogClient open(CatalogIntegration integration, ValidationBudget budget) {
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

  private static String safeSummary(String fallback, RuntimeException failure) {
    return failure instanceof CatalogAccessException accessFailure
            && accessFailure.getMessage() != null
            && !accessFailure.getMessage().isBlank()
        ? accessFailure.getMessage()
        : fallback;
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

  private record ValidationBudget(long deadlineNanos, LongSupplier nanoTime) {
    private static ValidationBudget start(Duration timeout, LongSupplier nanoTime) {
      long timeoutNanos = Math.max(0L, timeout.toNanos());
      return new ValidationBudget(nanoTime.getAsLong() + timeoutNanos, nanoTime);
    }

    private void check() {
      remainingNanos();
    }

    private <T> T call(Supplier<T> operation) {
      return call(operation, null);
    }

    private <T> T call(Supplier<T> operation, Consumer<T> abandonedResult) {
      long remainingNanos = remainingNanos();
      PropagatedContext context = PropagatedContext.capture();
      AbandonedResult<T> result = new AbandonedResult<>(context, abandonedResult);
      FutureTask<T> task =
          new FutureTask<>(() -> context.supply(() -> result.publish(operation.get())));
      Thread.ofVirtual().name("catalog-integration-upstream").start(task);
      try {
        T value = task.get(remainingNanos, TimeUnit.NANOSECONDS);
        result.claim();
        return value;
      } catch (TimeoutException failure) {
        result.abandon();
        task.cancel(true);
        throw timeout(failure);
      } catch (InterruptedException failure) {
        result.abandon();
        task.cancel(true);
        Thread.currentThread().interrupt();
        CancellationException cancelled =
            new CancellationException("Catalog upstream operation was cancelled");
        cancelled.initCause(failure);
        throw cancelled;
      } catch (ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof RuntimeException runtimeFailure) {
          throw runtimeFailure;
        }
        if (cause instanceof Error error) {
          throw error;
        }
        throw new CatalogAccessException(
            CatalogAccessException.Code.INTERNAL, "Catalog provider operation failed", cause);
      }
    }

    private void run(Runnable operation) {
      call(
          () -> {
            operation.run();
            return null;
          });
    }

    private long remainingNanos() {
      long remaining = deadlineNanos - nanoTime.getAsLong();
      if (remaining <= 0L) {
        throw timeout(null);
      }
      return remaining;
    }

    private static CatalogAccessException timeout(Throwable cause) {
      return new CatalogAccessException(
          CatalogAccessException.Code.TIMEOUT,
          "Catalog upstream operation exceeded the time limit",
          cause);
    }
  }

  private static final class AbandonedResult<T> {
    private final PropagatedContext context;
    private final Consumer<T> cleanup;
    private boolean abandoned;
    private boolean published;
    private T value;

    private AbandonedResult(PropagatedContext context, Consumer<T> cleanup) {
      this.context = context;
      this.cleanup = cleanup;
    }

    private T publish(T publishedValue) {
      synchronized (this) {
        if (!abandoned) {
          published = true;
          value = publishedValue;
          return publishedValue;
        }
      }
      cleanup(publishedValue);
      return publishedValue;
    }

    private synchronized void claim() {
      published = false;
      value = null;
    }

    private void abandon() {
      T abandonedValue;
      synchronized (this) {
        abandoned = true;
        if (!published) {
          return;
        }
        published = false;
        abandonedValue = value;
        value = null;
      }
      if (cleanup != null) {
        Thread.ofVirtual()
            .name("catalog-integration-cleanup")
            .start(() -> context.supply(() -> cleanup(abandonedValue)));
      }
    }

    private Void cleanup(T abandonedValue) {
      if (cleanup != null) {
        cleanup.accept(abandonedValue);
      }
      return null;
    }
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
