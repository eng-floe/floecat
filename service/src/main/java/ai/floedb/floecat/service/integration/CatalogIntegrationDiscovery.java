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
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheck;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheckType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationIssue;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Read-only discovery and validation against one persisted Catalog Integration. */
@ApplicationScoped
public class CatalogIntegrationDiscovery {
  private static final int MAX_VALIDATION_NAMESPACES = 100_000;

  @Inject CatalogIntegrationAccess access;

  ValidationResult validate(CatalogIntegration integration) {
    List<CatalogIntegrationValidationCheck> checks = new ArrayList<>();
    CatalogCapabilities capabilities = CatalogCapabilities.of();
    try (CatalogClient client = access.open(integration)) {
      capabilities = client.capabilities();
      if (!capabilities.supports(CatalogCapability.VALIDATE)) {
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
                CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
                "The catalog provider does not support connection validation."));
        addNotRunAfterConnection(checks);
        return new ValidationResult(false, checks, capabilities);
      }
      try {
        client.validate();
        checks.add(
            passed(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
                "The catalog endpoint accepted the configured authentication."));
      } catch (RuntimeException failure) {
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
                CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
                safeSummary("Catalog connection validation failed.", failure)));
        addNotRunAfterConnection(checks);
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
        validationTable = findFirstTable(client);
      } catch (RuntimeException failure) {
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
        checks.add(
            notRun(
                CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
                "Storage access was not tested because credential vending did not pass."));
        return new ValidationResult(false, checks, capabilities);
      }
      try {
        if (client.vendStorageCredentials(table).isEmpty()) {
          checks.add(
              failed(
                  CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
                  CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED,
                  "The catalog did not vend usable storage credentials for an upstream table."));
          checks.add(
              notRun(
                  CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
                  "Storage access was not tested because credential vending did not pass."));
          return new ValidationResult(false, checks, capabilities);
        }
        checks.add(
            passed(
                CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
                "The catalog vended scoped storage credentials."));
      } catch (RuntimeException failure) {
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
                CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED,
                safeSummary("Storage credential vending failed.", failure)));
        checks.add(
            notRun(
                CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
                "Storage access was not tested because credential vending did not pass."));
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
        client.validateStorageAccess(table);
        checks.add(
            passed(
                CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
                "Vended credentials can read upstream table storage."));
        return new ValidationResult(true, checks, capabilities);
      } catch (RuntimeException failure) {
        checks.add(
            failed(
                CatalogIntegrationValidationCheckType.CIVCT_STORAGE_ACCESS,
                CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_FAILED,
                safeSummary("Storage access validation failed.", failure)));
        return new ValidationResult(false, checks, capabilities);
      }
    } catch (RuntimeException failure) {
      checks.add(
          failed(
              CatalogIntegrationValidationCheckType.CIVCT_CATALOG_CONNECTION,
              CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
              safeSummary("The catalog client could not be opened.", failure)));
      addNotRunAfterConnection(checks);
      return new ValidationResult(false, checks, capabilities);
    }
  }

  List<NamespacePath> listNamespaces(CatalogIntegration integration, NamespacePath parent) {
    try (CatalogClient client = access.open(integration)) {
      require(client.capabilities(), CatalogCapability.LIST_NAMESPACES);
      return client.listNamespaces(parent).stream().distinct().sorted().toList();
    }
  }

  List<DiscoveredObject> listObjects(
      CatalogIntegration integration, NamespacePath namespace, Set<ObjectKind> requestedKinds) {
    try (CatalogClient client = access.open(integration)) {
      CatalogCapabilities capabilities = client.capabilities();
      Set<ObjectKind> kinds =
          requestedKinds.isEmpty() ? defaultKinds(capabilities) : EnumSet.copyOf(requestedKinds);
      List<DiscoveredObject> objects = new ArrayList<>();
      if (kinds.contains(ObjectKind.TABLE)) {
        require(capabilities, CatalogCapability.LIST_TABLES);
        client.listTables(namespace).stream()
            .map(name -> discovered(name, namespace, ObjectKind.TABLE))
            .forEach(objects::add);
      }
      if (kinds.contains(ObjectKind.VIEW)) {
        require(capabilities, CatalogCapability.LIST_VIEWS);
        client.listViews(namespace).stream()
            .map(name -> discovered(name, namespace, ObjectKind.VIEW))
            .forEach(objects::add);
      }
      return objects.stream().distinct().sorted().toList();
    }
  }

  private static Optional<CatalogObjectName> findFirstTable(CatalogClient client) {
    List<CatalogObjectName> rootTables = client.listTables(NamespacePath.root());
    if (!rootTables.isEmpty()) {
      return Optional.of(rootTables.stream().sorted().findFirst().orElseThrow());
    }
    Set<NamespacePath> seen = new HashSet<>();
    var pending = new ArrayDeque<NamespacePath>();
    pending.add(NamespacePath.root());
    while (!pending.isEmpty()) {
      NamespacePath parent = pending.removeFirst();
      for (NamespacePath namespace : client.listNamespaces(parent).stream().sorted().toList()) {
        if (!seen.add(namespace)) {
          continue;
        }
        if (seen.size() > MAX_VALIDATION_NAMESPACES) {
          throw new CatalogAccessException(
              CatalogAccessException.Code.UNAVAILABLE,
              "Catalog namespace inventory exceeds the validation limit");
        }
        List<CatalogObjectName> tables = client.listTables(namespace);
        if (!tables.isEmpty()) {
          return Optional.of(tables.stream().sorted().findFirst().orElseThrow());
        }
        pending.addLast(namespace);
      }
    }
    return Optional.empty();
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

  private static void addNotRunAfterConnection(List<CatalogIntegrationValidationCheck> checks) {
    checks.add(
        notRun(
            CatalogIntegrationValidationCheckType.CIVCT_DISCOVERY,
            "Discovery was not tested because catalog connection validation did not pass."));
    addNotRunAfterDiscovery(checks);
  }

  private static void addNotRunAfterDiscovery(List<CatalogIntegrationValidationCheck> checks) {
    checks.add(
        notRun(
            CatalogIntegrationValidationCheckType.CIVCT_CREDENTIAL_VENDING,
            "Credential vending was not tested because discovery did not pass."));
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
