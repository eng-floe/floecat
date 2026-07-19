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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_KIND;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_PARTITION_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_SORT_KEY;
import static ai.floedb.floecat.storage.kv.KvAttributes.ATTR_VERSION;

import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import ai.floedb.floecat.storage.kv.KvAttributes;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileJobIndexBackend implements ReconcileJobIndexBackend {
  private static final String KIND_GENERIC_POINTER = "Pointer";
  private static final String GENERIC_POINTER_GLOBAL_PK = "_ACCOUNT_DIR";
  private static final String ATTR_GENERIC_BLOB_URI = "blob_uri";
  private static final String ATTR_GENERIC_REFERENCE_KIND = "reference_kind";
  private static final String LEGACY_CLEANUP_MARKER_PARTITION = "reconcile-job-maintenance";
  private static final String LEGACY_CLEANUP_MARKER_SORT = "legacy-cleanup-manifest-v2";
  private static final String KIND_LEGACY_CLEANUP_MARKER = "ReconcileJobLegacyCleanupMigration";
  private static final String LEGACY_LOOKUP_MARKER_SORT = "legacy-lookup-v1";
  private static final String PHYSICAL_SORT_TOKEN_PREFIX = "dynamo-sort:";
  private static final String KIND_LEGACY_LOOKUP_MARKER = "ReconcileJobLegacyLookupMigration";
  private static final String KIND_LEGACY_MIGRATION_PROGRESS =
      "ReconcileJobLegacyMigrationProgress";
  private static final String LEGACY_CLEANUP_PROGRESS_SORT =
      LEGACY_CLEANUP_MARKER_SORT + "-progress";
  private static final String LEGACY_LOOKUP_PROGRESS_SORT = LEGACY_LOOKUP_MARKER_SORT + "-progress";
  private static final String ATTR_MIGRATION_OWNER = "migration_owner";
  private static final String ATTR_MIGRATION_FENCE = "migration_fence";
  private static final String ATTR_MIGRATION_LEASE_EXPIRES_AT_MS = "migration_lease_expires_at_ms";
  private static final String ATTR_MIGRATION_PAGE_TOKEN = "migration_page_token";
  private static final String ATTR_MIGRATION_CHANGED = "migration_changed";
  private static final String ATTR_MIGRATION_UNRESOLVABLE = "migration_unresolvable";
  private static final String ATTR_MIGRATION_CONFLICTED = "migration_conflicted";
  private static final String ATTR_MIGRATION_RETRYABLE = "migration_retryable";
  private static final String ATTR_MIGRATION_QUIET_PASS_COMPLETE = "migration_quiet_pass_complete";
  private static final int MAX_LEGACY_MIGRATION_PAGE_SIZE = 1_000;
  private static final int MAX_LEGACY_CLEANUP_FALLBACK_SCAN_PAGE_SIZE = 500;

  @Inject Instance<DynamoDbClientManager> dynamoDbClientManager;
  private final RefreshingDynamoCaller dynamoCaller = new RefreshingDynamoCaller();

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  @ConfigProperty(
      name = "floecat.gc.reconcile-jobs.legacy-cleanup-fallback-scan-page-size",
      defaultValue = "100")
  int legacyCleanupFallbackScanPageSize = 100;

  @ConfigProperty(
      name = "floecat.gc.reconcile-jobs.legacy-cleanup-manifest-max-bytes",
      defaultValue = "131072")
  int legacyCleanupManifestMaxBytes = 131072;

  private volatile boolean legacyCleanupMigrationMarkedComplete;
  private volatile boolean legacyLookupMigrationMarkedComplete;
  private volatile boolean legacyCleanupMigrationProgressCleaned;
  private volatile boolean legacyLookupMigrationProgressCleaned;

  public DynamoReconcileJobIndexBackend() {}

  public void bind(Supplier<DynamoDbClient> dynamoDbSupplier, String table) {
    dynamoCaller.bind(dynamoDbSupplier);
    this.table = table;
  }

  public void bind(DynamoDbClientManager manager, String table) {
    dynamoCaller.bind(manager);
    this.table = table;
  }

  @Override
  public Optional<JobIndexEntrySnapshot> loadIndexEntry(String pointerKey) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(pointerKey);
    if (lookupKey != null) {
      var currentKey = JobIndexBackendSupport.currentLookupStorageKey(lookupKey);
      Optional<JobIndexEntrySnapshot> current = loadLookupPointer(currentKey);
      if (current.isPresent()) {
        return current;
      }
      var legacyKey = JobIndexBackendSupport.legacyLookupStorageKey(lookupKey);
      // Keep reads side-effect-free. The scheduled legacy lookup migrator owns relocation and
      // conflict cleanup; readers only preserve current-first, legacy-second compatibility.
      return loadLookupPointer(legacyKey);
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(pointerKey);
    if (canonicalKey != null) {
      return loadCanonicalPointer(canonicalKey);
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(pointerKey);
    if (parentKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.parentPartitionKey(parentKey),
          JobIndexBackendSupport.parentSortKey(parentKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(pointerKey);
    if (connectorKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.connectorPartitionKey(connectorKey),
          JobIndexBackendSupport.connectorSortKey(connectorKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(pointerKey);
    if (globalStateKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
          JobIndexBackendSupport.globalStateSortKey(globalStateKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(pointerKey);
    if (accountStateKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
          JobIndexBackendSupport.accountStateSortKey(accountStateKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(pointerKey);
    if (connectorStateKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
          JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(pointerKey);
    if (dedupeKey != null) {
      return loadIndexPointer(
          JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
          JobIndexBackendSupport.dedupeSortKey(dedupeKey),
          JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    }
    return Optional.empty();
  }

  @Override
  public LegacyLookupMigrationPage migrateLegacyLookupEntries(int limit, String pageToken) {
    if (legacyMigrationComplete(LegacyMigration.LOOKUP)) {
      return new LegacyLookupMigrationPage(0, 0, 0, 0, "");
    }
    JobIndexQueryPage page =
        listIndexPointers(
            JobIndexBackendSupport.legacyLookupPartitionKey(),
            pageToken,
            boundedPageSize(limit, MAX_LEGACY_MIGRATION_PAGE_SIZE),
            JobIndexBackendSupport.ATTR_BLOB_URI);
    int migrated = 0;
    int conflicted = 0;
    int retryable = 0;
    for (JobIndexEntrySnapshot legacy : page.entries()) {
      var lookupKey = JobIndexBackendSupport.parseLookupKey(legacy.pointerKey());
      if (lookupKey == null || blank(legacy.blobUri()) || legacy.version() <= 0L) {
        conflicted++;
        continue;
      }
      switch (migrateLegacyLookup(lookupKey, legacy)) {
        case MIGRATED, CONFLICT_RESOLVED -> migrated++;
        case RETRYABLE_FAILURE -> retryable++;
      }
    }
    return new LegacyLookupMigrationPage(
        page.entries().size(), migrated, conflicted, retryable, page.nextPageToken());
  }

  @Override
  public boolean completeLegacyLookupMigration() {
    if (legacyMigrationComplete(LegacyMigration.LOOKUP)) {
      return true;
    }
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(LEGACY_CLEANUP_MARKER_PARTITION));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(LEGACY_LOOKUP_MARKER_SORT));
    item.put(ATTR_KIND, AttributeValue.fromS(KIND_LEGACY_LOOKUP_MARKER));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client -> client.putItem(PutItemRequest.builder().tableName(table).item(item).build()));
      legacyLookupMigrationMarkedComplete = true;
      cleanupLegacyMigrationProgress(LegacyMigration.LOOKUP);
      return true;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  @Override
  public boolean compareAndSetBatch(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    return compareAndSetBatch(batch, List.of());
  }

  @Override
  public boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    return compareAndSetDynamo(batch, extraPointerOps == null ? List.of() : extraPointerOps);
  }

  @Override
  public JobIndexQueryPage listCanonicalEntries(String accountId, int limit, String pageToken) {
    if (blank(accountId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.canonicalPartitionKey(accountId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_BLOB_URI);
  }

  @Override
  public JobIndexQueryPage listDedupeEntries(String accountId, int limit, String pageToken) {
    if (blank(accountId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.dedupePartitionKey(accountId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listParentEntries(
      String accountId, String parentJobId, int limit, String pageToken) {
    if (blank(accountId) || blank(parentJobId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.parentPartitionKey(accountId, parentJobId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listConnectorEntries(
      String accountId, String connectorId, int limit, String pageToken) {
    if (blank(accountId) || blank(connectorId)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.connectorPartitionKey(accountId, connectorId);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listGlobalStateEntries(String state, int limit, String pageToken) {
    if (blank(state)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.globalStatePartitionKey(state);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listAccountStateEntries(
      String accountId, String state, int limit, String pageToken) {
    if (blank(accountId) || blank(state)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey = JobIndexBackendSupport.accountStatePartitionKey(accountId, state);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public JobIndexQueryPage listConnectorStateEntries(
      String accountId, String connectorId, String state, int limit, String pageToken) {
    if (blank(accountId) || blank(connectorId) || blank(state)) {
      return new JobIndexQueryPage(List.of(), "");
    }
    String partitionKey =
        JobIndexBackendSupport.connectorStatePartitionKey(accountId, connectorId, state);
    return blank(partitionKey)
        ? new JobIndexQueryPage(List.of(), "")
        : listIndexPointers(
            partitionKey, pageToken, limit, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
  }

  @Override
  public ReconcileJobIndexCleanupManifest loadCleanupManifest(String canonicalPointerKey) {
    var key = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
    if (key == null) {
      return ReconcileJobIndexCleanupManifest.EMPTY;
    }
    var response =
        dynamoCaller.call(
            dynamoDbClientManager,
            client ->
                client.getItem(
                    GetItemRequest.builder()
                        .tableName(table)
                        .consistentRead(true)
                        .key(
                            Map.of(
                                ATTR_PARTITION_KEY,
                                AttributeValue.fromS(
                                    JobIndexBackendSupport.canonicalPartitionKey(key)),
                                ATTR_SORT_KEY,
                                AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key))))
                        .build()));
    if (!response.hasItem() || response.item().isEmpty()) {
      return ReconcileJobIndexCleanupManifest.EMPTY;
    }
    if (!legacyCleanupMigrationMarkedComplete()) {
      return ReconcileJobIndexCleanupManifest.EMPTY;
    }
    return mergeManifests(cleanupManifest(response.item()), legacyCleanupManifest(response.item()));
  }

  @Override
  public Optional<JobCleanupSession> beginJobCleanup(
      CanonicalPointerSnapshot expected, ReconcileJobIndexCleanupManifest fallbackManifest) {
    if (expected == null || !legacyCleanupMigrationMarkedComplete()) {
      return Optional.empty();
    }
    var key = JobIndexBackendSupport.parseCanonicalJobKey(expected.canonicalPointerKey());
    if (key == null) {
      return Optional.empty();
    }
    Map<String, AttributeValue> dynamoKey =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)),
            ATTR_SORT_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key)));
    final Map<String, AttributeValue> current;
    try {
      var response =
          dynamoCaller.call(
              dynamoDbClientManager,
              client ->
                  client.getItem(
                      GetItemRequest.builder()
                          .tableName(table)
                          .consistentRead(true)
                          .key(dynamoKey)
                          .build()));
      if (!response.hasItem() || response.item().isEmpty()) {
        return Optional.empty();
      }
      current = response.item();
    } catch (RuntimeException ignored) {
      return Optional.empty();
    }
    if (!expected
        .canonicalPointerKey()
        .equals(stringAttr(current, JobIndexBackendSupport.ATTR_POINTER_KEY))) {
      return Optional.empty();
    }
    boolean locked = boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
    long currentVersion = longAttr(current, ATTR_VERSION);
    String currentBlob = stringAttr(current, JobIndexBackendSupport.ATTR_BLOB_URI);
    if (!locked
        && (currentVersion != expected.version()
            || !java.util.Objects.equals(currentBlob, expected.blobUri()))) {
      return Optional.empty();
    }
    boolean hasLegacySets = hasLegacyCleanupSets(current);
    boolean legacyScanDrained =
        boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_DRAINED);
    boolean manifestComplete =
        boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE);
    boolean scanRequired =
        !legacyScanDrained
            && (boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED)
                || (!manifestComplete && !hasLegacySets));
    ReconcileJobIndexCleanupManifest fallback =
        fallbackManifest == null ? ReconcileJobIndexCleanupManifest.EMPTY : fallbackManifest;
    ReconcileJobIndexCleanupManifest boundedManifest =
        mergeManifests(
            mergeManifests(cleanupManifest(current), legacyCleanupManifest(current)), fallback);
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(expected.canonicalPointerKey());
    if (canonicalKey == null) {
      return Optional.empty();
    }
    if (!legacyScanDrained) {
      boundedManifest =
          mergeManifests(
              boundedManifest,
              new ReconcileJobIndexCleanupManifest(
                  List.of(Keys.reconcileJobLookupPointerByIdPrefix() + canonicalKey.jobSegment()),
                  List.of()));
    }
    if (!legacyScanDrained
        && ((locked && !boundedManifest.readyPointerKeys().isEmpty())
            || boundedManifest.readyPointerKeys().size()
                >= ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS - 1)) {
      // A retry must not keep replaying the same prefix of already-deleted ready rows. Force a
      // strongly consistent discovery for multi-phase cardinality and every resumed ready cleanup.
      scanRequired = true;
    }
    boolean boundedManifestValid =
        validCleanupManifest(boundedManifest)
            && (!hasLegacySets || manifestWithinMigrationLimit(boundedManifest));
    if (!boundedManifestValid && !legacyScanDrained) {
      scanRequired = true;
    }
    if (!boundedManifestValid) {
      if (!scanRequired) {
        return Optional.empty();
      }
      // The locked strong scan is authoritative. Do not let malformed legacy manifest entries
      // poison the claim before discovery can repair or conditionally remove their physical rows.
      boundedManifest =
          new ReconcileJobIndexCleanupManifest(
              List.of(Keys.reconcileJobLookupPointerByIdPrefix() + canonicalKey.jobSegment()),
              List.of());
    }

    Map<String, AttributeValue> lockedItem = current;
    if (!locked
        || hasLegacySets
        || (scanRequired
            && !boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED))) {
      Map<String, String> names = new HashMap<>();
      names.put("#v", ATTR_VERSION);
      names.put("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY);
      names.put("#blob", JobIndexBackendSupport.ATTR_BLOB_URI);
      names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
      if (scanRequired) {
        names.put("#scan", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED);
        names.put("#drained", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_DRAINED);
      } else {
        names.put("#complete", JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE);
        names.put("#idx", JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS);
        names.put("#ready", JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS);
      }
      names.put("#legacyIdx", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS);
      names.put("#legacyReady", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS);
      Map<String, AttributeValue> values = new HashMap<>();
      values.put(":expected", AttributeValue.fromN(Long.toString(currentVersion)));
      values.put(":next", AttributeValue.fromN(Long.toString(currentVersion + 1L)));
      values.put(":canonical", AttributeValue.fromS(expected.canonicalPointerKey()));
      values.put(":blob", AttributeValue.fromS(currentBlob));
      values.put(":true", AttributeValue.fromBool(true));
      if (!scanRequired) {
        values.put(":idx", stringListValue(boundedManifest.indexPointerKeys()));
        values.put(":ready", stringListValue(boundedManifest.readyPointerKeys()));
      }
      String update =
          "SET #v = :next, #lock = :true"
              + (scanRequired ? ", #scan = :true" : "")
              + (scanRequired ? "" : ", #complete = :true, #idx = :idx, #ready = :ready")
              + " REMOVE #legacyIdx, #legacyReady"
              + (scanRequired ? ", #drained" : "");
      String condition =
          "#v = :expected AND #pointer = :canonical AND #blob = :blob AND "
              + (locked ? "#lock = :true" : "attribute_not_exists(#lock)");
      try {
        var response =
            dynamoCaller.call(
                dynamoDbClientManager,
                client ->
                    client.updateItem(
                        UpdateItemRequest.builder()
                            .tableName(table)
                            .key(dynamoKey)
                            .updateExpression(update)
                            .conditionExpression(condition)
                            .expressionAttributeNames(names)
                            .expressionAttributeValues(values)
                            .returnValues(ReturnValue.ALL_NEW)
                            .build()));
        lockedItem = response.attributes();
        currentVersion = longAttr(lockedItem, ATTR_VERSION);
        currentBlob = stringAttr(lockedItem, JobIndexBackendSupport.ATTR_BLOB_URI);
      } catch (ConditionalCheckFailedException ignored) {
        return Optional.empty();
      } catch (RuntimeException ignored) {
        if (!scanRequired && hasLegacySets) {
          markCleanupPromotionForBoundedScan(
              dynamoKey, expected.canonicalPointerKey(), currentBlob, currentVersion, locked);
        }
        return Optional.empty();
      }
    }

    if (scanRequired) {
      drainOwnedCleanupScanPage(
          dynamoKey, expected.canonicalPointerKey(), lockedItem, currentVersion);
      // The durable cursor (or drained marker) makes the next GC pass resume safely. Never expose
      // a partial manifest to the delete planner.
      return Optional.empty();
    }
    ReconcileJobIndexCleanupManifest manifest = boundedManifest;
    if ((!legacyScanDrained && manifest.isEmpty()) || !validCleanupManifest(manifest)) {
      return Optional.empty();
    }
    return Optional.of(
        new JobCleanupSession(
            new CanonicalPointerSnapshot(
                expected.canonicalPointerKey(), currentBlob, currentVersion),
            manifest,
            true,
            legacyScanDrained));
  }

  @Override
  public LegacyCleanupMigrationPage migrateLegacyCleanupManifests(int limit, String pageToken) {
    if (legacyMigrationComplete(LegacyMigration.CLEANUP)) {
      return new LegacyCleanupMigrationPage(0, 0, 0, 0, 0, "");
    }

    List<String> kinds =
        List.of(
            JobIndexBackendSupport.KIND_CANONICAL_JOB,
            JobIndexBackendSupport.KIND_LOOKUP,
            JobIndexBackendSupport.KIND_DEDUPE,
            JobIndexBackendSupport.KIND_PARENT,
            JobIndexBackendSupport.KIND_CONNECTOR,
            JobIndexBackendSupport.KIND_GLOBAL_STATE,
            JobIndexBackendSupport.KIND_ACCOUNT_STATE,
            JobIndexBackendSupport.KIND_CONNECTOR_STATE,
            DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY);
    Map<String, AttributeValue> values = new HashMap<>();
    List<String> kindTokens = new ArrayList<>();
    for (int i = 0; i < kinds.size(); i++) {
      String token = ":kind" + i;
      kindTokens.add(token);
      values.put(token, AttributeValue.fromS(kinds.get(i)));
    }
    Map<String, String> names =
        Map.ofEntries(
            Map.entry("#pk", ATTR_PARTITION_KEY),
            Map.entry("#sk", ATTR_SORT_KEY),
            Map.entry("#kind", ATTR_KIND),
            Map.entry("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY),
            Map.entry("#canonical", JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY),
            Map.entry("#blob", JobIndexBackendSupport.ATTR_BLOB_URI),
            Map.entry("#ready", DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY),
            Map.entry("#complete", JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE));
    ScanRequest.Builder scan =
        ScanRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(boundedPageSize(limit, MAX_LEGACY_MIGRATION_PAGE_SIZE))
            .expressionAttributeNames(names)
            .filterExpression(
                "#kind IN ("
                    + String.join(", ", kindTokens)
                    + ") AND (attribute_exists(#canonical) OR attribute_exists(#blob))")
            .projectionExpression("#pk, #sk, #kind, #pointer, #canonical, #blob, #ready, #complete")
            .expressionAttributeValues(values);
    ReadyQueueBackendSupport.ReadyRowCursor cursor =
        ReadyQueueBackendSupport.decodeCursor(pageToken);
    if (cursor != null) {
      scan.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY,
              AttributeValue.fromS(cursor.partitionKey()),
              ATTR_SORT_KEY,
              AttributeValue.fromS(cursor.sortKey())));
    }

    var response = dynamoCaller.call(dynamoDbClientManager, client -> client.scan(scan.build()));
    Map<String, LegacyCleanupManifestBuilder> manifests = new java.util.LinkedHashMap<>();
    java.util.LinkedHashSet<String> canonicalPointerKeys = new java.util.LinkedHashSet<>();
    int unresolvable = 0;
    int conflicted = 0;
    for (var item : response.items()) {
      String canonicalPointerKey = canonicalPointerForCleanupItem(item);
      if (blank(canonicalPointerKey)
          || JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey) == null) {
        unresolvable++;
        continue;
      }
      LegacyCleanupManifestBuilder manifest =
          manifests.computeIfAbsent(
              canonicalPointerKey, ignored -> new LegacyCleanupManifestBuilder());
      var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
      manifest.indexKeys.add(
          Keys.reconcileJobLookupPointerByIdPrefix() + canonicalKey.jobSegment());
      String kind = stringAttr(item, ATTR_KIND);
      String pointerKey = stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY);
      if (JobIndexBackendSupport.KIND_CANONICAL_JOB.equals(kind)
          && canonicalPointerKey.equals(pointerKey)) {
        if (!physicalKeyMatchesCanonical(item, canonicalKey)) {
          conflicted++;
          continue;
        }
        if (!boolAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE)) {
          canonicalPointerKeys.add(canonicalPointerKey);
        }
        continue;
      }
      String readyKey = stringAttr(item, DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY);
      if (DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY.equals(kind)) {
        if (blank(readyKey) || !physicalKeyMatchesReady(item, readyKey)) {
          conflicted++;
          continue;
        }
        manifest.readyKeys.add(readyKey);
        continue;
      }
      if (blank(pointerKey)
          || !validCleanupIndexPointer(kind, pointerKey)
          || !physicalKeyMatchesIndex(item, kind, pointerKey)) {
        conflicted++;
        continue;
      }
      manifest.indexKeys.add(pointerKey);
    }

    int updated = 0;
    int retryable = 0;
    for (var entry : manifests.entrySet()) {
      LegacyCleanupMergeOutcome outcome =
          mergeLegacyCleanupManifest(entry.getKey(), entry.getValue().build());
      if (outcome == LegacyCleanupMergeOutcome.UPDATED) {
        updated++;
      } else if (outcome == LegacyCleanupMergeOutcome.CONFLICTED) {
        conflicted++;
      } else if (outcome == LegacyCleanupMergeOutcome.RETRYABLE) {
        retryable++;
      }
    }

    String nextToken = "";
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextToken =
          ReadyQueueBackendSupport.encodeCursor(
              stringAttr(response.lastEvaluatedKey(), ATTR_PARTITION_KEY),
              stringAttr(response.lastEvaluatedKey(), ATTR_SORT_KEY));
    }
    return new LegacyCleanupMigrationPage(
        response.scannedCount(),
        updated,
        unresolvable,
        conflicted,
        retryable,
        nextToken,
        List.copyOf(canonicalPointerKeys));
  }

  @Override
  public boolean completeLegacyCleanupMigration() {
    if (legacyMigrationComplete(LegacyMigration.CLEANUP)) {
      return true;
    }
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(LEGACY_CLEANUP_MARKER_PARTITION));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(LEGACY_CLEANUP_MARKER_SORT));
    item.put(ATTR_KIND, AttributeValue.fromS(KIND_LEGACY_CLEANUP_MARKER));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client -> client.putItem(PutItemRequest.builder().tableName(table).item(item).build()));
      legacyCleanupMigrationMarkedComplete = true;
      cleanupLegacyMigrationProgress(LegacyMigration.CLEANUP);
      return true;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  @Override
  public Optional<LegacyMigrationLease> acquireLegacyMigrationLease(
      LegacyMigration migration, String ownerId, long nowMs, long leaseDurationMs) {
    if (migration == null || blank(ownerId) || legacyMigrationComplete(migration)) {
      return Optional.empty();
    }
    LegacyMigrationStorage storage = legacyMigrationStorage(migration);
    Map<String, String> names =
        Map.of(
            "#kind", ATTR_KIND,
            "#owner", ATTR_MIGRATION_OWNER,
            "#fence", ATTR_MIGRATION_FENCE,
            "#expires", ATTR_MIGRATION_LEASE_EXPIRES_AT_MS);
    Map<String, AttributeValue> values =
        Map.of(
            ":kind", AttributeValue.fromS(KIND_LEGACY_MIGRATION_PROGRESS),
            ":owner", AttributeValue.fromS(ownerId),
            ":now", AttributeValue.fromN(Long.toString(nowMs)),
            ":expires",
                AttributeValue.fromN(
                    Long.toString(leaseExpiresAt(nowMs, Math.max(1L, leaseDurationMs)))),
            ":one", AttributeValue.fromN("1"));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder()
                      .transactItems(
                          TransactWriteItem.builder()
                              .conditionCheck(
                                  software.amazon.awssdk.services.dynamodb.model.ConditionCheck
                                      .builder()
                                      .tableName(table)
                                      .key(legacyMigrationKey(storage.markerSortKey()))
                                      .conditionExpression("attribute_not_exists(#pk)")
                                      .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                                      .build())
                              .build(),
                          TransactWriteItem.builder()
                              .update(
                                  Update.builder()
                                      .tableName(table)
                                      .key(legacyMigrationKey(storage.progressSortKey()))
                                      .conditionExpression(
                                          "attribute_not_exists(#owner) OR #owner = :owner OR #expires <= :now")
                                      .updateExpression(
                                          "SET #kind = :kind, #owner = :owner, #expires = :expires ADD #fence :one")
                                      .expressionAttributeNames(names)
                                      .expressionAttributeValues(values)
                                      .build())
                              .build())
                      .build()));
      var response =
          dynamoCaller.call(
              dynamoDbClientManager,
              client ->
                  client.getItem(
                      GetItemRequest.builder()
                          .tableName(table)
                          .consistentRead(true)
                          .key(legacyMigrationKey(storage.progressSortKey()))
                          .build()));
      if (!response.hasItem()
          || response.item().isEmpty()
          || !ownerId.equals(stringAttr(response.item(), ATTR_MIGRATION_OWNER))) {
        return Optional.empty();
      }
      Map<String, AttributeValue> attributes = response.item();
      long fence = longAttr(attributes, ATTR_MIGRATION_FENCE);
      if (fence <= 0L) {
        return Optional.empty();
      }
      return Optional.of(new LegacyMigrationLease(fence, migrationProgress(attributes)));
    } catch (RuntimeException ignored) {
      legacyMigrationComplete(migration);
      return Optional.empty();
    }
  }

  @Override
  public boolean checkpointLegacyMigration(
      LegacyMigration migration,
      String ownerId,
      long fence,
      LegacyMigrationProgress progress,
      long nowMs,
      long leaseDurationMs) {
    if (migration == null || blank(ownerId) || fence <= 0L || progress == null) {
      return false;
    }
    LegacyMigrationStorage storage = legacyMigrationStorage(migration);
    Map<String, String> names =
        Map.ofEntries(
            Map.entry("#owner", ATTR_MIGRATION_OWNER),
            Map.entry("#fence", ATTR_MIGRATION_FENCE),
            Map.entry("#expires", ATTR_MIGRATION_LEASE_EXPIRES_AT_MS),
            Map.entry("#page", ATTR_MIGRATION_PAGE_TOKEN),
            Map.entry("#changed", ATTR_MIGRATION_CHANGED),
            Map.entry("#unresolvable", ATTR_MIGRATION_UNRESOLVABLE),
            Map.entry("#conflicted", ATTR_MIGRATION_CONFLICTED),
            Map.entry("#retryable", ATTR_MIGRATION_RETRYABLE),
            Map.entry("#quiet", ATTR_MIGRATION_QUIET_PASS_COMPLETE));
    Map<String, AttributeValue> values =
        Map.ofEntries(
            Map.entry(":owner", AttributeValue.fromS(ownerId)),
            Map.entry(":fence", AttributeValue.fromN(Long.toString(fence))),
            Map.entry(":now", AttributeValue.fromN(Long.toString(nowMs))),
            Map.entry(
                ":expires",
                AttributeValue.fromN(
                    Long.toString(leaseExpiresAt(nowMs, Math.max(1L, leaseDurationMs))))),
            Map.entry(":page", AttributeValue.fromS(progress.pageToken())),
            Map.entry(":changed", AttributeValue.fromN(Integer.toString(progress.changed()))),
            Map.entry(
                ":unresolvable", AttributeValue.fromN(Integer.toString(progress.unresolvable()))),
            Map.entry(":conflicted", AttributeValue.fromN(Integer.toString(progress.conflicted()))),
            Map.entry(":retryable", AttributeValue.fromN(Integer.toString(progress.retryable()))),
            Map.entry(":quiet", AttributeValue.fromBool(progress.quietPassComplete())));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(table)
                      .key(legacyMigrationKey(storage.progressSortKey()))
                      .conditionExpression(
                          "#owner = :owner AND #fence = :fence AND #expires >= :now")
                      .updateExpression(
                          "SET #expires = :expires, #page = :page, #changed = :changed, #unresolvable = :unresolvable, #conflicted = :conflicted, #retryable = :retryable, #quiet = :quiet")
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .build()));
      return true;
    } catch (ConditionalCheckFailedException ignored) {
      return false;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  @Override
  public boolean completeLegacyMigration(
      LegacyMigration migration, String ownerId, long fence, long nowMs) {
    if (migration == null || blank(ownerId) || fence <= 0L) {
      return false;
    }
    if (legacyMigrationComplete(migration)) {
      return true;
    }
    LegacyMigrationStorage storage = legacyMigrationStorage(migration);
    Map<String, String> checkNames = new HashMap<>();
    checkNames.put("#owner", ATTR_MIGRATION_OWNER);
    checkNames.put("#fence", ATTR_MIGRATION_FENCE);
    checkNames.put("#expires", ATTR_MIGRATION_LEASE_EXPIRES_AT_MS);
    checkNames.put("#changed", ATTR_MIGRATION_CHANGED);
    checkNames.put("#retryable", ATTR_MIGRATION_RETRYABLE);
    checkNames.put("#quiet", ATTR_MIGRATION_QUIET_PASS_COMPLETE);
    Map<String, AttributeValue> checkValues =
        Map.ofEntries(
            Map.entry(":owner", AttributeValue.fromS(ownerId)),
            Map.entry(":fence", AttributeValue.fromN(Long.toString(fence))),
            Map.entry(":now", AttributeValue.fromN(Long.toString(nowMs))),
            Map.entry(":zero", AttributeValue.fromN("0")),
            Map.entry(":quiet", AttributeValue.fromBool(true)));
    String quietCondition =
        "#owner = :owner AND #fence = :fence AND #expires >= :now AND #quiet = :quiet AND #changed = :zero AND #retryable = :zero";
    if (migration == LegacyMigration.CLEANUP) {
      checkNames.put("#unresolvable", ATTR_MIGRATION_UNRESOLVABLE);
      checkNames.put("#conflicted", ATTR_MIGRATION_CONFLICTED);
      quietCondition += " AND #unresolvable = :zero AND #conflicted = :zero";
    }
    final String completionCondition = quietCondition;

    Map<String, AttributeValue> marker = new HashMap<>();
    marker.put(ATTR_PARTITION_KEY, AttributeValue.fromS(LEGACY_CLEANUP_MARKER_PARTITION));
    marker.put(ATTR_SORT_KEY, AttributeValue.fromS(storage.markerSortKey()));
    marker.put(ATTR_KIND, AttributeValue.fromS(storage.markerKind()));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder()
                      .transactItems(
                          TransactWriteItem.builder()
                              .delete(
                                  Delete.builder()
                                      .tableName(table)
                                      .key(legacyMigrationKey(storage.progressSortKey()))
                                      .conditionExpression(completionCondition)
                                      .expressionAttributeNames(checkNames)
                                      .expressionAttributeValues(checkValues)
                                      .build())
                              .build(),
                          TransactWriteItem.builder()
                              .put(
                                  Put.builder()
                                      .tableName(table)
                                      .item(marker)
                                      .conditionExpression("attribute_not_exists(#pk)")
                                      .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                                      .build())
                              .build())
                      .build()));
      markLegacyMigrationComplete(migration);
      markLegacyMigrationProgressCleaned(migration);
      return true;
    } catch (RuntimeException ignored) {
      return legacyMigrationComplete(migration);
    }
  }

  @Override
  public boolean legacyMigrationComplete(LegacyMigration migration) {
    if (migration == null || !legacyMigrationMarkedComplete(migration)) {
      return false;
    }
    cleanupLegacyMigrationProgress(migration);
    return true;
  }

  @Override
  public boolean legacyCleanupMigrationComplete() {
    return legacyMigrationComplete(LegacyMigration.CLEANUP);
  }

  private record LegacyMigrationStorage(
      String progressSortKey, String markerSortKey, String markerKind) {}

  private static LegacyMigrationStorage legacyMigrationStorage(LegacyMigration migration) {
    return switch (migration) {
      case CLEANUP ->
          new LegacyMigrationStorage(
              LEGACY_CLEANUP_PROGRESS_SORT, LEGACY_CLEANUP_MARKER_SORT, KIND_LEGACY_CLEANUP_MARKER);
      case LOOKUP ->
          new LegacyMigrationStorage(
              LEGACY_LOOKUP_PROGRESS_SORT, LEGACY_LOOKUP_MARKER_SORT, KIND_LEGACY_LOOKUP_MARKER);
    };
  }

  private static Map<String, AttributeValue> legacyMigrationKey(String sortKey) {
    return Map.of(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(LEGACY_CLEANUP_MARKER_PARTITION),
        ATTR_SORT_KEY,
        AttributeValue.fromS(sortKey));
  }

  private static LegacyMigrationProgress migrationProgress(Map<String, AttributeValue> attributes) {
    return new LegacyMigrationProgress(
        stringAttr(attributes, ATTR_MIGRATION_PAGE_TOKEN),
        nonNegativeIntAttr(attributes, ATTR_MIGRATION_CHANGED),
        nonNegativeIntAttr(attributes, ATTR_MIGRATION_UNRESOLVABLE),
        nonNegativeIntAttr(attributes, ATTR_MIGRATION_CONFLICTED),
        nonNegativeIntAttr(attributes, ATTR_MIGRATION_RETRYABLE),
        boolAttr(attributes, ATTR_MIGRATION_QUIET_PASS_COMPLETE));
  }

  private boolean legacyMigrationMarkedComplete(LegacyMigration migration) {
    return switch (migration) {
      case CLEANUP -> legacyCleanupMigrationMarkedComplete();
      case LOOKUP -> legacyLookupMigrationMarkedComplete();
    };
  }

  private void markLegacyMigrationComplete(LegacyMigration migration) {
    if (migration == LegacyMigration.CLEANUP) {
      legacyCleanupMigrationMarkedComplete = true;
    } else {
      legacyLookupMigrationMarkedComplete = true;
    }
  }

  private void markLegacyMigrationProgressCleaned(LegacyMigration migration) {
    if (migration == LegacyMigration.CLEANUP) {
      legacyCleanupMigrationProgressCleaned = true;
    } else {
      legacyLookupMigrationProgressCleaned = true;
    }
  }

  private boolean legacyMigrationProgressCleaned(LegacyMigration migration) {
    return migration == LegacyMigration.CLEANUP
        ? legacyCleanupMigrationProgressCleaned
        : legacyLookupMigrationProgressCleaned;
  }

  private void cleanupLegacyMigrationProgress(LegacyMigration migration) {
    if (legacyMigrationProgressCleaned(migration)) {
      return;
    }
    LegacyMigrationStorage storage = legacyMigrationStorage(migration);
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.deleteItem(
                  DeleteItemRequest.builder()
                      .tableName(table)
                      .key(legacyMigrationKey(storage.progressSortKey()))
                      .build()));
      markLegacyMigrationProgressCleaned(migration);
    } catch (RuntimeException ignored) {
      // Completion remains authoritative. Retry cleanup on the next status check.
    }
  }

  private static long leaseExpiresAt(long nowMs, long leaseDurationMs) {
    if (leaseDurationMs > 0L && nowMs > Long.MAX_VALUE - leaseDurationMs) {
      return Long.MAX_VALUE;
    }
    return nowMs + leaseDurationMs;
  }

  private boolean legacyCleanupMigrationMarkedComplete() {
    if (legacyCleanupMigrationMarkedComplete) {
      return true;
    }
    try {
      var response =
          dynamoCaller.call(
              dynamoDbClientManager,
              client ->
                  client.getItem(
                      GetItemRequest.builder()
                          .tableName(table)
                          .consistentRead(true)
                          .key(
                              Map.of(
                                  ATTR_PARTITION_KEY,
                                  AttributeValue.fromS(LEGACY_CLEANUP_MARKER_PARTITION),
                                  ATTR_SORT_KEY,
                                  AttributeValue.fromS(LEGACY_CLEANUP_MARKER_SORT)))
                          .build()));
      legacyCleanupMigrationMarkedComplete = response.hasItem() && !response.item().isEmpty();
      return legacyCleanupMigrationMarkedComplete;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  private boolean legacyLookupMigrationMarkedComplete() {
    if (legacyLookupMigrationMarkedComplete) {
      return true;
    }
    try {
      var response =
          dynamoCaller.call(
              dynamoDbClientManager,
              client ->
                  client.getItem(
                      GetItemRequest.builder()
                          .tableName(table)
                          .consistentRead(true)
                          .key(
                              Map.of(
                                  ATTR_PARTITION_KEY,
                                  AttributeValue.fromS(LEGACY_CLEANUP_MARKER_PARTITION),
                                  ATTR_SORT_KEY,
                                  AttributeValue.fromS(LEGACY_LOOKUP_MARKER_SORT)))
                          .build()));
      legacyLookupMigrationMarkedComplete = response.hasItem() && !response.item().isEmpty();
      return legacyLookupMigrationMarkedComplete;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  private String canonicalPointerForCleanupItem(Map<String, AttributeValue> item) {
    String kind = stringAttr(item, ATTR_KIND);
    String pointerKey = stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY);
    if (JobIndexBackendSupport.KIND_CANONICAL_JOB.equals(kind)) {
      if (JobIndexBackendSupport.parseCanonicalJobKey(pointerKey) != null) {
        return pointerKey;
      }
      if (JobIndexBackendSupport.parseLookupKey(pointerKey) != null) {
        return stringAttr(item, JobIndexBackendSupport.ATTR_BLOB_URI);
      }
    }
    String canonical = stringAttr(item, JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY);
    if (!blank(canonical)) {
      return canonical;
    }
    if (JobIndexBackendSupport.KIND_LOOKUP.equals(kind)) {
      return stringAttr(item, JobIndexBackendSupport.ATTR_BLOB_URI);
    }
    return "";
  }

  private static boolean validCleanupIndexPointer(String kind, String pointerKey) {
    if (blank(pointerKey)) {
      return false;
    }
    return switch (kind) {
      case JobIndexBackendSupport.KIND_CANONICAL_JOB, JobIndexBackendSupport.KIND_LOOKUP ->
          JobIndexBackendSupport.parseLookupKey(pointerKey) != null;
      case JobIndexBackendSupport.KIND_DEDUPE ->
          JobIndexBackendSupport.parseDedupeKey(pointerKey) != null;
      case JobIndexBackendSupport.KIND_PARENT ->
          JobIndexBackendSupport.parseParentKey(pointerKey) != null;
      case JobIndexBackendSupport.KIND_CONNECTOR ->
          JobIndexBackendSupport.parseConnectorKey(pointerKey) != null;
      case JobIndexBackendSupport.KIND_GLOBAL_STATE ->
          JobIndexBackendSupport.parseGlobalStateKey(pointerKey) != null;
      case JobIndexBackendSupport.KIND_ACCOUNT_STATE ->
          JobIndexBackendSupport.parseAccountStateKey(pointerKey) != null;
      case JobIndexBackendSupport.KIND_CONNECTOR_STATE ->
          JobIndexBackendSupport.parseConnectorStateKey(pointerKey) != null;
      default -> false;
    };
  }

  private static boolean physicalKeyMatchesCanonical(
      Map<String, AttributeValue> item, JobIndexBackendSupport.CanonicalJobKey key) {
    return physicalKeyMatches(
        item,
        JobIndexBackendSupport.canonicalPartitionKey(key),
        JobIndexBackendSupport.canonicalSortKey(key));
  }

  private static boolean physicalKeyMatchesReady(
      Map<String, AttributeValue> item, String readyPointerKey) {
    ReadyQueueBackendSupport.ReadyQueueRow row =
        ReadyQueueBackendSupport.toReadyQueueRow(readyPointerKey);
    return row != null && physicalKeyMatches(item, row.partitionKey(), row.sortKey());
  }

  private static boolean physicalKeyMatchesIndex(
      Map<String, AttributeValue> item, String kind, String pointerKey) {
    if (blank(kind) || blank(pointerKey)) {
      return false;
    }
    if (JobIndexBackendSupport.KIND_CANONICAL_JOB.equals(kind)
        || JobIndexBackendSupport.KIND_LOOKUP.equals(kind)) {
      var key = JobIndexBackendSupport.parseLookupKey(pointerKey);
      return key != null
          && JobIndexBackendSupport.lookupReadStorageKeys(key).stream()
              .anyMatch(
                  storageKey ->
                      physicalKeyMatches(item, storageKey.partitionKey(), storageKey.sortKey()));
    }
    if (JobIndexBackendSupport.KIND_DEDUPE.equals(kind)) {
      var key = JobIndexBackendSupport.parseDedupeKey(pointerKey);
      return key != null
          && physicalKeyMatches(
              item,
              JobIndexBackendSupport.dedupePartitionKey(key),
              JobIndexBackendSupport.dedupeSortKey(key));
    }
    if (JobIndexBackendSupport.KIND_PARENT.equals(kind)) {
      var key = JobIndexBackendSupport.parseParentKey(pointerKey);
      return key != null
          && physicalKeyMatches(
              item,
              JobIndexBackendSupport.parentPartitionKey(key),
              JobIndexBackendSupport.parentSortKey(key));
    }
    if (JobIndexBackendSupport.KIND_CONNECTOR.equals(kind)) {
      var key = JobIndexBackendSupport.parseConnectorKey(pointerKey);
      return key != null
          && physicalKeyMatches(
              item,
              JobIndexBackendSupport.connectorPartitionKey(key),
              JobIndexBackendSupport.connectorSortKey(key));
    }
    if (JobIndexBackendSupport.KIND_GLOBAL_STATE.equals(kind)) {
      var key = JobIndexBackendSupport.parseGlobalStateKey(pointerKey);
      return key != null
          && physicalKeyMatches(
              item,
              JobIndexBackendSupport.globalStatePartitionKey(key),
              JobIndexBackendSupport.globalStateSortKey(key));
    }
    if (JobIndexBackendSupport.KIND_ACCOUNT_STATE.equals(kind)) {
      var key = JobIndexBackendSupport.parseAccountStateKey(pointerKey);
      return key != null
          && physicalKeyMatches(
              item,
              JobIndexBackendSupport.accountStatePartitionKey(key),
              JobIndexBackendSupport.accountStateSortKey(key));
    }
    if (JobIndexBackendSupport.KIND_CONNECTOR_STATE.equals(kind)) {
      var key = JobIndexBackendSupport.parseConnectorStateKey(pointerKey);
      return key != null
          && physicalKeyMatches(
              item,
              JobIndexBackendSupport.connectorStatePartitionKey(key),
              JobIndexBackendSupport.connectorStateSortKey(key));
    }
    return false;
  }

  private static boolean physicalKeyMatches(
      Map<String, AttributeValue> item, String expectedPartitionKey, String expectedSortKey) {
    return !blank(expectedPartitionKey)
        && !blank(expectedSortKey)
        && expectedPartitionKey.equals(stringAttr(item, ATTR_PARTITION_KEY))
        && expectedSortKey.equals(stringAttr(item, ATTR_SORT_KEY));
  }

  private static boolean validCleanupManifest(ReconcileJobIndexCleanupManifest manifest) {
    if (manifest == null) {
      return false;
    }
    for (String pointerKey : manifest.indexPointerKeys()) {
      if (JobIndexBackendSupport.parseLookupKey(pointerKey) == null
          && JobIndexBackendSupport.parseDedupeKey(pointerKey) == null
          && JobIndexBackendSupport.parseParentKey(pointerKey) == null
          && JobIndexBackendSupport.parseConnectorKey(pointerKey) == null
          && JobIndexBackendSupport.parseGlobalStateKey(pointerKey) == null
          && JobIndexBackendSupport.parseAccountStateKey(pointerKey) == null
          && JobIndexBackendSupport.parseConnectorStateKey(pointerKey) == null) {
        return false;
      }
    }
    return manifest.readyPointerKeys().stream()
        .allMatch(pointerKey -> ReadyQueueBackendSupport.toReadyQueueRow(pointerKey) != null);
  }

  private void drainOwnedCleanupScanPage(
      Map<String, AttributeValue> canonicalKey,
      String canonicalPointerKey,
      Map<String, AttributeValue> lockedItem,
      long expectedVersion) {
    Map<String, String> names =
        Map.ofEntries(
            Map.entry("#pk", ATTR_PARTITION_KEY),
            Map.entry("#sk", ATTR_SORT_KEY),
            Map.entry("#v", ATTR_VERSION),
            Map.entry("#kind", ATTR_KIND),
            Map.entry("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY),
            Map.entry("#canonical", JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY),
            Map.entry("#blob", JobIndexBackendSupport.ATTR_BLOB_URI),
            Map.entry("#ready", DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY));
    List<String> kinds =
        List.of(
            JobIndexBackendSupport.KIND_CANONICAL_JOB,
            JobIndexBackendSupport.KIND_LOOKUP,
            JobIndexBackendSupport.KIND_DEDUPE,
            JobIndexBackendSupport.KIND_PARENT,
            JobIndexBackendSupport.KIND_CONNECTOR,
            JobIndexBackendSupport.KIND_GLOBAL_STATE,
            JobIndexBackendSupport.KIND_ACCOUNT_STATE,
            JobIndexBackendSupport.KIND_CONNECTOR_STATE,
            DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":canonical", AttributeValue.fromS(canonicalPointerKey));
    List<String> kindTokens = new ArrayList<>();
    for (int i = 0; i < kinds.size(); i++) {
      String token = ":kind" + i;
      kindTokens.add(token);
      values.put(token, AttributeValue.fromS(kinds.get(i)));
    }
    ScanRequest.Builder scan =
        ScanRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(
                boundedPageSize(
                    legacyCleanupFallbackScanPageSize, MAX_LEGACY_CLEANUP_FALLBACK_SCAN_PAGE_SIZE))
            .filterExpression(
                "#kind IN ("
                    + String.join(", ", kindTokens)
                    + ") AND (#canonical = :canonical OR #blob = :canonical)")
            .projectionExpression("#pk, #sk, #v, #kind, #pointer, #canonical, #blob, #ready")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values);
    String storedCursor =
        stringAttr(lockedItem, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_CURSOR);
    ReadyQueueBackendSupport.ReadyRowCursor cursor =
        ReadyQueueBackendSupport.decodeCursor(storedCursor);
    if (cursor != null) {
      scan.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY,
              AttributeValue.fromS(cursor.partitionKey()),
              ATTR_SORT_KEY,
              AttributeValue.fromS(cursor.sortKey())));
    }

    final ScanResponse response;
    try {
      response = dynamoCaller.call(dynamoDbClientManager, client -> client.scan(scan.build()));
    } catch (RuntimeException ignored) {
      return;
    }
    var parsedCanonical = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
    if (parsedCanonical == null) {
      return;
    }
    List<Map<String, AttributeValue>> ownedRows = new ArrayList<>();
    for (Map<String, AttributeValue> item : response.items()) {
      String ownerAttribute = cleanupOwnerAttribute(item);
      if (blank(ownerAttribute) || !canonicalPointerKey.equals(stringAttr(item, ownerAttribute))) {
        // The broad filter can match a non-authoritative reference attribute.
        continue;
      }
      if (physicalKeyMatchesCanonical(item, parsedCanonical)) {
        // The canonical is the final row deleted by GC after this bounded scan is drained.
        continue;
      }
      if (blank(stringAttr(item, ATTR_PARTITION_KEY)) || blank(stringAttr(item, ATTR_SORT_KEY))) {
        return;
      }
      ownedRows.add(item);
    }
    if (!deleteOwnedCleanupRows(canonicalKey, canonicalPointerKey, expectedVersion, ownedRows)) {
      return;
    }

    String nextCursor = "";
    if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
      nextCursor =
          ReadyQueueBackendSupport.encodeCursor(
              stringAttr(response.lastEvaluatedKey(), ATTR_PARTITION_KEY),
              stringAttr(response.lastEvaluatedKey(), ATTR_SORT_KEY));
    }
    checkpointOwnedCleanupScan(canonicalKey, canonicalPointerKey, expectedVersion, nextCursor);
  }

  private boolean deleteOwnedCleanupRows(
      Map<String, AttributeValue> canonicalKey,
      String canonicalPointerKey,
      long expectedVersion,
      List<Map<String, AttributeValue>> rows) {
    int chunkSize = ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS - 1;
    try {
      for (int offset = 0; offset < rows.size(); offset += chunkSize) {
        int end = Math.min(rows.size(), offset + chunkSize);
        List<TransactWriteItem> tx = new ArrayList<>(end - offset + 1);
        tx.add(
            TransactWriteItem.builder()
                .conditionCheck(
                    software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                        .tableName(table)
                        .key(canonicalKey)
                        .conditionExpression(
                            "#v = :expected AND #pointer = :canonical AND #lock = :true")
                        .expressionAttributeNames(
                            Map.of(
                                "#v", ATTR_VERSION,
                                "#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY,
                                "#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS))
                        .expressionAttributeValues(
                            Map.of(
                                ":expected",
                                AttributeValue.fromN(Long.toString(expectedVersion)),
                                ":canonical",
                                AttributeValue.fromS(canonicalPointerKey),
                                ":true",
                                AttributeValue.fromBool(true)))
                        .build())
                .build());
        for (int index = offset; index < end; index++) {
          tx.add(buildOwnedCleanupDelete(canonicalPointerKey, rows.get(index)));
        }
        dynamoCaller.callVoid(
            dynamoDbClientManager,
            client ->
                client.transactWriteItems(
                    TransactWriteItemsRequest.builder().transactItems(tx).build()));
      }
      return true;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  private TransactWriteItem buildOwnedCleanupDelete(
      String canonicalPointerKey, Map<String, AttributeValue> item) {
    String ownerAttribute = cleanupOwnerAttribute(item);
    Map<String, String> names = new HashMap<>();
    names.put("#pk", ATTR_PARTITION_KEY);
    names.put("#owner", ownerAttribute);
    names.put("#v", ATTR_VERSION);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":canonical", AttributeValue.fromS(canonicalPointerKey));
    AttributeValue scannedVersion = item.get(ATTR_VERSION);
    String versionCondition;
    if (scannedVersion == null) {
      versionCondition = "attribute_not_exists(#v)";
    } else {
      values.put(":version", scannedVersion);
      versionCondition = "#v = :version";
    }
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, item.get(ATTR_PARTITION_KEY),
                        ATTR_SORT_KEY, item.get(ATTR_SORT_KEY)))
                .conditionExpression(
                    "attribute_not_exists(#pk) OR (#owner = :canonical AND "
                        + versionCondition
                        + ")")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build())
        .build();
  }

  private boolean checkpointOwnedCleanupScan(
      Map<String, AttributeValue> canonicalKey,
      String canonicalPointerKey,
      long expectedVersion,
      String nextCursor) {
    boolean drained = blank(nextCursor);
    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    names.put("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY);
    names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
    names.put("#scan", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED);
    names.put("#cursor", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_CURSOR);
    names.put("#drained", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_DRAINED);
    names.put("#legacyIdx", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS);
    names.put("#legacyReady", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":expected", AttributeValue.fromN(Long.toString(expectedVersion)));
    values.put(":next", AttributeValue.fromN(Long.toString(expectedVersion + 1L)));
    values.put(":canonical", AttributeValue.fromS(canonicalPointerKey));
    values.put(":true", AttributeValue.fromBool(true));
    String updateExpression;
    if (drained) {
      names.put("#complete", JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE);
      names.put("#idx", JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS);
      names.put("#ready", JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS);
      values.put(":empty", AttributeValue.fromL(List.of()));
      updateExpression =
          "SET #v = :next, #complete = :true, #drained = :true, #idx = :empty, #ready = :empty "
              + "REMOVE #scan, #cursor, #legacyIdx, #legacyReady";
    } else {
      values.put(":cursor", AttributeValue.fromS(nextCursor));
      updateExpression =
          "SET #v = :next, #scan = :true, #cursor = :cursor "
              + "REMOVE #drained, #legacyIdx, #legacyReady";
    }
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(table)
                      .key(canonicalKey)
                      .conditionExpression(
                          "#v = :expected AND #pointer = :canonical AND #lock = :true")
                      .updateExpression(updateExpression)
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .build()));
      return true;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  private boolean markCleanupPromotionForBoundedScan(
      Map<String, AttributeValue> canonicalKey,
      String canonicalPointerKey,
      String expectedBlob,
      long expectedVersion,
      boolean alreadyLocked) {
    Map<String, String> names =
        Map.ofEntries(
            Map.entry("#v", ATTR_VERSION),
            Map.entry("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY),
            Map.entry("#blob", JobIndexBackendSupport.ATTR_BLOB_URI),
            Map.entry("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS),
            Map.entry("#scan", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED),
            Map.entry("#drained", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_DRAINED),
            Map.entry("#legacyIdx", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS),
            Map.entry(
                "#legacyReady", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS));
    Map<String, AttributeValue> values =
        Map.ofEntries(
            Map.entry(":expected", AttributeValue.fromN(Long.toString(expectedVersion))),
            Map.entry(":next", AttributeValue.fromN(Long.toString(expectedVersion + 1L))),
            Map.entry(":canonical", AttributeValue.fromS(canonicalPointerKey)),
            Map.entry(":blob", AttributeValue.fromS(expectedBlob)),
            Map.entry(":true", AttributeValue.fromBool(true)));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(table)
                      .key(canonicalKey)
                      .conditionExpression(
                          "#v = :expected AND #pointer = :canonical AND #blob = :blob AND "
                              + (alreadyLocked ? "#lock = :true" : "attribute_not_exists(#lock)"))
                      .updateExpression(
                          "SET #v = :next, #lock = :true, #scan = :true "
                              + "REMOVE #drained, #legacyIdx, #legacyReady")
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .build()));
      return true;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  private static String cleanupOwnerAttribute(Map<String, AttributeValue> item) {
    String kind = stringAttr(item, ATTR_KIND);
    if (JobIndexBackendSupport.KIND_CANONICAL_JOB.equals(kind)) {
      return JobIndexBackendSupport.ATTR_BLOB_URI;
    }
    if (JobIndexBackendSupport.KIND_LOOKUP.equals(kind)) {
      return JobIndexBackendSupport.ATTR_BLOB_URI;
    }
    return JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY;
  }

  private static boolean hasLegacyCleanupSets(Map<String, AttributeValue> item) {
    return item != null
        && (item.containsKey(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS)
            || item.containsKey(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS));
  }

  private static boolean legacyCleanupSetStorageValid(Map<String, AttributeValue> item) {
    if (item == null) {
      return true;
    }
    AttributeValue indexes =
        item.get(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS);
    AttributeValue ready = item.get(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS);
    return (indexes == null || indexes.hasSs() || indexes.hasL())
        && (ready == null || ready.hasSs() || ready.hasL());
  }

  private LegacyCleanupMergeOutcome mergeLegacyCleanupManifest(
      String canonicalPointerKey, ReconcileJobIndexCleanupManifest discovered) {
    var key = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
    if (key == null || discovered == null || discovered.isEmpty()) {
      return LegacyCleanupMergeOutcome.CONFLICTED;
    }
    Map<String, AttributeValue> dynamoKey =
        Map.of(
            ATTR_PARTITION_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)),
            ATTR_SORT_KEY,
            AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key)));
    final Map<String, AttributeValue> item;
    try {
      var response =
          dynamoCaller.call(
              dynamoDbClientManager,
              client ->
                  client.getItem(
                      GetItemRequest.builder()
                          .tableName(table)
                          .consistentRead(true)
                          .key(dynamoKey)
                          .build()));
      if (!response.hasItem() || response.item().isEmpty()) {
        return purgeOrphanedLegacyFootprint(canonicalPointerKey, discovered);
      }
      item = response.item();
    } catch (RuntimeException ignored) {
      return LegacyCleanupMergeOutcome.RETRYABLE;
    }
    if (!canonicalPointerKey.equals(stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY))) {
      return LegacyCleanupMergeOutcome.CONFLICTED;
    }
    if (boolAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED)
        && !hasLegacyCleanupSets(item)) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }
    if (!legacyCleanupSetStorageValid(item)) {
      return markLegacyCleanupScanRequired(dynamoKey, canonicalPointerKey, item);
    }

    // A canonical's references can be spread across many table-scan pages. Accumulate fragments
    // in hidden sets and expose/promote them only after the global completion marker is durable;
    // setting the normal manifest complete here would make a later fragment look authoritative.
    ReconcileJobIndexCleanupManifest normal = cleanupManifest(item);
    ReconcileJobIndexCleanupManifest legacy = legacyCleanupManifest(item);
    boolean complete = boolAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE);
    List<String> indexAdditions =
        missingMigrationKeys(
            discovered.indexPointerKeys(),
            complete
                ? mergeManifests(normal, legacy).indexPointerKeys()
                : legacy.indexPointerKeys());
    List<String> readyAdditions =
        missingMigrationKeys(
            discovered.readyPointerKeys(),
            complete
                ? mergeManifests(normal, legacy).readyPointerKeys()
                : legacy.readyPointerKeys());
    if (indexAdditions.isEmpty() && readyAdditions.isEmpty()) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }
    ReconcileJobIndexCleanupManifest accumulated =
        mergeManifests(
            normal,
            mergeManifests(
                legacy, new ReconcileJobIndexCleanupManifest(indexAdditions, readyAdditions)));
    if (!validCleanupManifest(accumulated) || !manifestWithinMigrationLimit(accumulated)) {
      return markLegacyCleanupScanRequired(dynamoKey, canonicalPointerKey, item);
    }

    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    names.put("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY);
    names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
    Map<String, AttributeValue> values = new HashMap<>();
    long currentVersion = longAttr(item, ATTR_VERSION);
    values.put(":v", AttributeValue.fromN(Long.toString(currentVersion)));
    values.put(":next", AttributeValue.fromN(Long.toString(currentVersion + 1L)));
    values.put(":canonical", AttributeValue.fromS(canonicalPointerKey));
    List<String> setActions = new ArrayList<>();
    setActions.add("#v = :next");
    List<String> addActions = new ArrayList<>();
    if (!indexAdditions.isEmpty()) {
      names.put("#legacyIdx", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS);
      AttributeValue stored =
          item.get(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS);
      if (stored != null && stored.hasL()) {
        values.put(
            ":legacyIdx",
            AttributeValue.fromSs(
                mergeManifests(
                        legacy, new ReconcileJobIndexCleanupManifest(indexAdditions, List.of()))
                    .indexPointerKeys()));
        setActions.add("#legacyIdx = :legacyIdx");
      } else {
        values.put(":legacyIdx", AttributeValue.fromSs(indexAdditions));
        addActions.add("#legacyIdx :legacyIdx");
      }
    }
    if (!readyAdditions.isEmpty()) {
      names.put("#legacyReady", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS);
      AttributeValue stored =
          item.get(JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS);
      if (stored != null && stored.hasL()) {
        values.put(
            ":legacyReady",
            AttributeValue.fromSs(
                mergeManifests(
                        legacy, new ReconcileJobIndexCleanupManifest(List.of(), readyAdditions))
                    .readyPointerKeys()));
        setActions.add("#legacyReady = :legacyReady");
      } else {
        values.put(":legacyReady", AttributeValue.fromSs(readyAdditions));
        addActions.add("#legacyReady :legacyReady");
      }
    }
    String updateExpression = "SET " + String.join(", ", setActions);
    if (!addActions.isEmpty()) {
      updateExpression += " ADD " + String.join(", ", addActions);
    }
    final String migrationUpdateExpression = updateExpression;
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(table)
                      .key(dynamoKey)
                      .updateExpression(migrationUpdateExpression)
                      .conditionExpression(
                          "#v = :v AND #pointer = :canonical AND attribute_not_exists(#lock)")
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .build()));
      return LegacyCleanupMergeOutcome.UPDATED;
    } catch (ConditionalCheckFailedException ignored) {
      return classifyLegacyCleanupConditionalConflict(dynamoKey, canonicalPointerKey, discovered);
    } catch (RuntimeException ignored) {
      return markLegacyCleanupScanRequired(dynamoKey, canonicalPointerKey, item);
    }
  }

  private LegacyCleanupMergeOutcome markLegacyCleanupScanRequired(
      Map<String, AttributeValue> dynamoKey,
      String canonicalPointerKey,
      Map<String, AttributeValue> item) {
    Map<String, String> names =
        Map.ofEntries(
            Map.entry("#v", ATTR_VERSION),
            Map.entry("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY),
            Map.entry("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS),
            Map.entry("#scan", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED),
            Map.entry("#legacyIdx", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS),
            Map.entry(
                "#legacyReady", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS));
    long version = longAttr(item, ATTR_VERSION);
    Map<String, AttributeValue> values =
        Map.of(
            ":v", AttributeValue.fromN(Long.toString(version)),
            ":next", AttributeValue.fromN(Long.toString(version + 1L)),
            ":canonical", AttributeValue.fromS(canonicalPointerKey),
            ":true", AttributeValue.fromBool(true));
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(table)
                      .key(dynamoKey)
                      .updateExpression(
                          "SET #v = :next, #scan = :true REMOVE #legacyIdx, #legacyReady")
                      .conditionExpression(
                          "#v = :v AND #pointer = :canonical AND attribute_not_exists(#lock)")
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .build()));
      return LegacyCleanupMergeOutcome.UPDATED;
    } catch (ConditionalCheckFailedException ignored) {
      return LegacyCleanupMergeOutcome.RETRYABLE;
    } catch (RuntimeException ignored) {
      return LegacyCleanupMergeOutcome.RETRYABLE;
    }
  }

  private LegacyCleanupMergeOutcome classifyLegacyCleanupConditionalConflict(
      Map<String, AttributeValue> dynamoKey,
      String canonicalPointerKey,
      ReconcileJobIndexCleanupManifest expectedManifest) {
    final Map<String, AttributeValue> current;
    try {
      var response =
          dynamoCaller.call(
              dynamoDbClientManager,
              client ->
                  client.getItem(
                      GetItemRequest.builder()
                          .tableName(table)
                          .consistentRead(true)
                          .key(dynamoKey)
                          .build()));
      if (!response.hasItem() || response.item().isEmpty()) {
        return purgeOrphanedLegacyFootprint(canonicalPointerKey, expectedManifest);
      }
      current = response.item();
    } catch (RuntimeException ignored) {
      return LegacyCleanupMergeOutcome.RETRYABLE;
    }
    if (!canonicalPointerKey.equals(stringAttr(current, JobIndexBackendSupport.ATTR_POINTER_KEY))) {
      return LegacyCleanupMergeOutcome.CONFLICTED;
    }
    if (boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED)
        && !hasLegacyCleanupSets(current)) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }
    ReconcileJobIndexCleanupManifest represented =
        boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE)
            ? mergeManifests(cleanupManifest(current), legacyCleanupManifest(current))
            : legacyCleanupManifest(current);
    if (represented.indexPointerKeys().containsAll(expectedManifest.indexPointerKeys())
        && represented.readyPointerKeys().containsAll(expectedManifest.readyPointerKeys())) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }
    return LegacyCleanupMergeOutcome.RETRYABLE;
  }

  private static List<String> missingMigrationKeys(List<String> desired, List<String> existing) {
    java.util.LinkedHashSet<String> missing = new java.util.LinkedHashSet<>(desired);
    missing.removeAll(existing);
    return List.copyOf(missing);
  }

  private LegacyCleanupMergeOutcome purgeOrphanedLegacyFootprint(
      String canonicalPointerKey, ReconcileJobIndexCleanupManifest discovered) {
    boolean deleted = false;
    for (String pointerKey : discovered.indexPointerKeys()) {
      final JobIndexEntrySnapshot current;
      try {
        current = loadIndexEntry(pointerKey).orElse(null);
      } catch (RuntimeException ignored) {
        return LegacyCleanupMergeOutcome.RETRYABLE;
      }
      if (current == null || !canonicalPointerKey.equals(current.blobUri())) {
        continue;
      }
      ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
          new ReconcileJobIndexStore.JobIndexWriteBatch(
              List.of(
                  new ReconcileJobIndexStore.JobIndexCheckAbsent(canonicalPointerKey),
                  new ReconcileJobIndexStore.JobIndexDelete(
                      current.pointerKey(),
                      current.version(),
                      canonicalPointerKey,
                      current.lookupStoragePartitionKey())),
              ReconcileJobIndexStore.ReadyQueueMutation.empty());
      if (!compareAndSetDynamo(deleteBatch, List.of())) {
        return LegacyCleanupMergeOutcome.RETRYABLE;
      }
      deleted = true;
    }
    for (String readyPointerKey : discovered.readyPointerKeys()) {
      ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
          new ReconcileJobIndexStore.JobIndexWriteBatch(
              List.of(new ReconcileJobIndexStore.JobIndexCheckAbsent(canonicalPointerKey)),
              new ReconcileJobIndexStore.ReadyQueueMutation(List.of(), List.of(readyPointerKey)));
      if (!compareAndSetDynamo(deleteBatch, List.of())) {
        return LegacyCleanupMergeOutcome.RETRYABLE;
      }
      deleted = true;
    }
    return deleted ? LegacyCleanupMergeOutcome.UPDATED : LegacyCleanupMergeOutcome.UNCHANGED;
  }

  private static List<String> concat(List<String> left, List<String> right) {
    List<String> values = new ArrayList<>(left.size() + right.size());
    values.addAll(left);
    values.addAll(right);
    return values;
  }

  private static ReconcileJobIndexCleanupManifest mergeManifests(
      ReconcileJobIndexCleanupManifest left, ReconcileJobIndexCleanupManifest right) {
    ReconcileJobIndexCleanupManifest first =
        left == null ? ReconcileJobIndexCleanupManifest.EMPTY : left;
    ReconcileJobIndexCleanupManifest second =
        right == null ? ReconcileJobIndexCleanupManifest.EMPTY : right;
    return new ReconcileJobIndexCleanupManifest(
        concat(first.indexPointerKeys(), second.indexPointerKeys()),
        concat(first.readyPointerKeys(), second.readyPointerKeys()));
  }

  private enum LegacyCleanupMergeOutcome {
    UPDATED,
    UNCHANGED,
    CONFLICTED,
    RETRYABLE
  }

  private static final class LegacyCleanupManifestBuilder {
    private final java.util.LinkedHashSet<String> indexKeys = new java.util.LinkedHashSet<>();
    private final java.util.LinkedHashSet<String> readyKeys = new java.util.LinkedHashSet<>();

    private ReconcileJobIndexCleanupManifest build() {
      return new ReconcileJobIndexCleanupManifest(List.copyOf(indexKeys), List.copyOf(readyKeys));
    }
  }

  private boolean compareAndSetDynamo(
      ReconcileJobIndexStore.JobIndexWriteBatch batch, List<PointerStore.CasOp> extraPointerOps) {
    List<TransactWriteItem> tx = new ArrayList<>();
    if (batch != null) {
      for (ReconcileJobIndexStore.JobIndexWriteOp op : batch.writes()) {
        if (op instanceof ReconcileJobIndexStore.JobIndexUpsert upsert) {
          tx.addAll(buildPointerUpsert(upsert));
        } else if (op instanceof ReconcileJobIndexStore.JobIndexDelete delete) {
          tx.addAll(buildPointerDelete(delete));
        } else if (op instanceof ReconcileJobIndexStore.JobIndexCheck check) {
          tx.add(buildPointerCheck(check));
        } else if (op instanceof ReconcileJobIndexStore.JobIndexCheckAbsent check) {
          tx.addAll(buildPointerCheckAbsent(check.pointerKey()));
        }
      }
      for (var upsert : batch.readyMutation().upserts()) {
        ReadyQueueBackendSupport.ReadyQueueRow row =
            ReadyQueueBackendSupport.toReadyQueueRow(
                upsert.readyPointerKey(), upsert.canonicalPointerKey());
        if (row != null) {
          tx.add(buildReadyUpsert(row));
        }
      }
      for (String readyPointerKey : batch.readyMutation().deletes()) {
        // Resolve the delete key from the ready pointer alone: a delete fires when a canonical
        // mutation (requeue/fail/terminal) supersedes the old ready pointer, and the canonical it
        // referenced is being rewritten in this same transaction. Passing a blank canonical here
        // made
        // the row resolve to null, so no delete item was added and the old ready row leaked — the
        // backlog source the prune path is meant to drain.
        ReadyQueueBackendSupport.ReadyQueueRow row =
            ReadyQueueBackendSupport.toReadyQueueRow(readyPointerKey);
        if (row != null) {
          tx.add(buildReadyDelete(row));
        }
      }
    }
    for (PointerStore.CasOp op : extraPointerOps) {
      if (op instanceof PointerStore.CasUpsert upsert) {
        tx.add(buildGenericPointerUpsert(upsert));
      } else if (op instanceof PointerStore.CasDelete delete) {
        tx.add(buildGenericPointerDelete(delete.key(), delete.expectedVersion()));
      } else if (op instanceof PointerStore.CasCheck check) {
        tx.add(buildGenericPointerCheck(check.key(), check.expectedVersion()));
      } else if (op instanceof PointerStore.CasCheckAbsent check) {
        tx.add(buildGenericPointerCheckAbsent(check.key()));
      }
    }
    if (tx.size() > ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS) {
      throw new IllegalArgumentException(
          "DynamoDB transaction exceeds "
              + ReconcileJobWriteLimits.MAX_TRANSACTION_ITEMS
              + " items: "
              + tx.size());
    }
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder().transactItems(tx).build()));
      return true;
    } catch (TransactionCanceledException e) {
      return false;
    }
  }

  private TransactWriteItem buildReadyUpsert(ReadyQueueBackendSupport.ReadyQueueRow row) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(KvAttributes.ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()));
    item.put(KvAttributes.ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey()));
    item.put(
        KvAttributes.ATTR_KIND,
        AttributeValue.fromS(DynamoReconcileReadyQueueBackend.KIND_READY_ENTRY));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY,
        AttributeValue.fromS(row.entry().readyPointerKey()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_CANONICAL_POINTER_KEY,
        AttributeValue.fromS(row.entry().canonicalPointerKey()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_ACCOUNT_ID,
        AttributeValue.fromS(row.entry().accountId()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_JOB_ID, AttributeValue.fromS(row.entry().jobId()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_DUE_AT_MS,
        AttributeValue.fromN(Long.toString(row.entry().dueAtMs())));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_INDEX_TYPE,
        AttributeValue.fromS(row.entry().indexType().name()));
    item.put(
        DynamoReconcileReadyQueueBackend.ATTR_FILTER_VALUE,
        AttributeValue.fromS(row.entry().filterValue()));
    return TransactWriteItem.builder()
        .put(Put.builder().tableName(table).item(item).build())
        .build();
  }

  private TransactWriteItem buildReadyDelete(ReadyQueueBackendSupport.ReadyQueueRow row) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        KvAttributes.ATTR_PARTITION_KEY, AttributeValue.fromS(row.partitionKey()),
                        KvAttributes.ATTR_SORT_KEY, AttributeValue.fromS(row.sortKey())))
                .build())
        .build();
  }

  private Optional<JobIndexEntrySnapshot> loadCanonicalPointer(
      JobIndexBackendSupport.CanonicalJobKey key) {
    var response =
        dynamoCaller.call(
            dynamoDbClientManager,
            client ->
                client.getItem(
                    GetItemRequest.builder()
                        .tableName(table)
                        .consistentRead(true)
                        .key(
                            Map.of(
                                ATTR_PARTITION_KEY,
                                AttributeValue.fromS(
                                    JobIndexBackendSupport.canonicalPartitionKey(key)),
                                ATTR_SORT_KEY,
                                AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key))))
                        .build()));
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new JobIndexEntrySnapshot(
            stringAttr(response.item(), JobIndexBackendSupport.ATTR_POINTER_KEY),
            stringAttr(response.item(), JobIndexBackendSupport.ATTR_BLOB_URI),
            longAttr(response.item(), ATTR_VERSION),
            boolAttr(response.item(), JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS)));
  }

  private Optional<JobIndexEntrySnapshot> loadIndexPointer(
      String partitionKey, String sortKey, String referenceAttributeName) {
    var response =
        dynamoCaller.call(
            dynamoDbClientManager,
            client ->
                client.getItem(
                    GetItemRequest.builder()
                        .tableName(table)
                        .consistentRead(true)
                        .key(
                            Map.of(
                                ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                                ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                        .build()));
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new JobIndexEntrySnapshot(
            stringAttr(response.item(), JobIndexBackendSupport.ATTR_POINTER_KEY),
            stringAttr(response.item(), referenceAttributeName),
            longAttr(response.item(), ATTR_VERSION)));
  }

  private Optional<JobIndexEntrySnapshot> loadLookupPointer(
      JobIndexBackendSupport.LookupStorageKey key) {
    return loadIndexPointer(key.partitionKey(), key.sortKey(), JobIndexBackendSupport.ATTR_BLOB_URI)
        .map(
            snapshot ->
                new JobIndexEntrySnapshot(
                    snapshot.pointerKey(),
                    snapshot.blobUri(),
                    snapshot.version(),
                    key.partitionKey()));
  }

  private LegacyLookupMigrationOutcome migrateLegacyLookup(
      JobIndexBackendSupport.LookupKey lookupKey, JobIndexEntrySnapshot legacy) {
    var currentKey = JobIndexBackendSupport.currentLookupStorageKey(lookupKey);
    var legacyKey = JobIndexBackendSupport.legacyLookupStorageKey(lookupKey);
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(currentKey.partitionKey()));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(currentKey.sortKey()));
    item.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_LOOKUP));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(legacy.version())));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(lookupKey.pointerKey()));
    item.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(legacy.blobUri()));
    TransactWriteItem put =
        TransactWriteItem.builder()
            .put(
                Put.builder()
                    .tableName(table)
                    .item(item)
                    .conditionExpression("attribute_not_exists(#pk)")
                    .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                    .build())
            .build();
    TransactWriteItem delete =
        buildDeleteWithReference(
            legacyKey.partitionKey(),
            legacyKey.sortKey(),
            legacy.version(),
            JobIndexBackendSupport.ATTR_BLOB_URI,
            legacy.blobUri());
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder().transactItems(List.of(put, delete)).build()));
      return LegacyLookupMigrationOutcome.MIGRATED;
    } catch (TransactionCanceledException ignored) {
      Optional<JobIndexEntrySnapshot> current;
      try {
        current = loadLookupPointer(currentKey);
      } catch (RuntimeException ignoredRead) {
        return LegacyLookupMigrationOutcome.RETRYABLE_FAILURE;
      }
      if (current.isEmpty()) {
        return LegacyLookupMigrationOutcome.RETRYABLE_FAILURE;
      }
      if (!legacy.blobUri().equals(current.get().blobUri())) {
        return deleteConflictingLegacyLookup(legacyKey, legacy);
      }
      try {
        dynamoCaller.callVoid(
            dynamoDbClientManager,
            client ->
                client.transactWriteItems(
                    TransactWriteItemsRequest.builder().transactItems(List.of(delete)).build()));
      } catch (RuntimeException ignoredDelete) {
        try {
          if (loadLookupPointer(legacyKey).isEmpty()) {
            return LegacyLookupMigrationOutcome.MIGRATED;
          }
        } catch (RuntimeException ignoredRead) {
          // The legacy row may still exist; leave the migration retryable.
        }
        return LegacyLookupMigrationOutcome.RETRYABLE_FAILURE;
      }
      return LegacyLookupMigrationOutcome.MIGRATED;
    } catch (RuntimeException ignored) {
      return LegacyLookupMigrationOutcome.RETRYABLE_FAILURE;
    }
  }

  private LegacyLookupMigrationOutcome deleteConflictingLegacyLookup(
      JobIndexBackendSupport.LookupStorageKey legacyKey, JobIndexEntrySnapshot legacy) {
    TransactWriteItem delete =
        buildDeleteWithReference(
            legacyKey.partitionKey(),
            legacyKey.sortKey(),
            legacy.version(),
            JobIndexBackendSupport.ATTR_BLOB_URI,
            legacy.blobUri());
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.transactWriteItems(
                  TransactWriteItemsRequest.builder().transactItems(List.of(delete)).build()));
      return LegacyLookupMigrationOutcome.CONFLICT_RESOLVED;
    } catch (RuntimeException ignoredDelete) {
      try {
        if (loadLookupPointer(legacyKey).isEmpty()) {
          return LegacyLookupMigrationOutcome.CONFLICT_RESOLVED;
        }
      } catch (RuntimeException ignoredRead) {
        // The conflicting legacy row may still exist; leave the migration retryable.
      }
      return LegacyLookupMigrationOutcome.RETRYABLE_FAILURE;
    }
  }

  private enum LegacyLookupMigrationOutcome {
    MIGRATED,
    CONFLICT_RESOLVED,
    RETRYABLE_FAILURE
  }

  private JobIndexQueryPage listIndexPointers(
      String partitionKey, String pageToken, int limit, String referenceAttributeName) {
    QueryRequest.Builder query =
        QueryRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, limit))
            .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
            .keyConditionExpression("#pk = :pk")
            .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(partitionKey)));
    String resumeSortKey = sortKeyFromPageToken(pageToken);
    if (!resumeSortKey.isBlank()) {
      query.exclusiveStartKey(
          Map.of(
              ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
              ATTR_SORT_KEY, AttributeValue.fromS(resumeSortKey)));
    }
    var response = dynamoCaller.call(dynamoDbClientManager, client -> client.query(query.build()));
    List<JobIndexEntrySnapshot> pointers = new ArrayList<>(response.items().size());
    for (var item : response.items()) {
      String pointerKey = stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY);
      pointers.add(
          new JobIndexEntrySnapshot(
              pointerKey,
              stringAttr(item, referenceAttributeName),
              longAttr(item, ATTR_VERSION),
              JobIndexBackendSupport.parseCanonicalJobKey(pointerKey) != null
                  && boolAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS)));
    }
    String nextPageToken = "";
    if (response.lastEvaluatedKey() != null
        && !response.lastEvaluatedKey().isEmpty()
        && !response.items().isEmpty()) {
      String physicalSortKey = stringAttr(response.lastEvaluatedKey(), ATTR_SORT_KEY);
      if (!physicalSortKey.isBlank()) {
        nextPageToken = PHYSICAL_SORT_TOKEN_PREFIX + physicalSortKey;
      }
    }
    return new JobIndexQueryPage(List.copyOf(pointers), nextPageToken);
  }

  private String sortKeyFromPageToken(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return "";
    }
    if (pageToken.startsWith(PHYSICAL_SORT_TOKEN_PREFIX)) {
      return pageToken.substring(PHYSICAL_SORT_TOKEN_PREFIX.length());
    }
    var lookupKey = JobIndexBackendSupport.parseLookupKey(pageToken);
    if (lookupKey != null) {
      return JobIndexBackendSupport.lookupSortKey(lookupKey);
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(pageToken);
    if (canonicalKey != null) {
      return JobIndexBackendSupport.canonicalSortKey(canonicalKey);
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(pageToken);
    if (parentKey != null) {
      return JobIndexBackendSupport.parentSortKey(parentKey);
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(pageToken);
    if (connectorKey != null) {
      return JobIndexBackendSupport.connectorSortKey(connectorKey);
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(pageToken);
    if (globalStateKey != null) {
      return JobIndexBackendSupport.globalStateSortKey(globalStateKey);
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(pageToken);
    if (accountStateKey != null) {
      return JobIndexBackendSupport.accountStateSortKey(accountStateKey);
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(pageToken);
    if (connectorStateKey != null) {
      return JobIndexBackendSupport.connectorStateSortKey(connectorStateKey);
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(pageToken);
    if (dedupeKey != null) {
      return JobIndexBackendSupport.dedupeSortKey(dedupeKey);
    }
    return "";
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private List<TransactWriteItem> buildPointerUpsert(ReconcileJobIndexStore.JobIndexUpsert upsert) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(upsert.pointerKey());
    if (lookupKey != null) {
      var currentKey = JobIndexBackendSupport.currentLookupStorageKey(lookupKey);
      return List.of(
          buildIndexUpsert(
              currentKey.partitionKey(),
              currentKey.sortKey(),
              JobIndexBackendSupport.KIND_LOOKUP,
              upsert,
              JobIndexBackendSupport.ATTR_BLOB_URI),
          DynamoReconcileJobLookupCompatibility.legacyCheckAbsent(table, lookupKey));
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(upsert.pointerKey());
    if (canonicalKey != null) {
      return List.of(buildCanonicalUpsert(canonicalKey, upsert));
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(upsert.pointerKey());
    if (parentKey != null) {
      return List.of(
          buildIndexUpsert(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey),
              JobIndexBackendSupport.KIND_PARENT,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(upsert.pointerKey());
    if (connectorKey != null) {
      return List.of(
          buildIndexUpsert(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey),
              JobIndexBackendSupport.KIND_CONNECTOR,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(upsert.pointerKey());
    if (globalStateKey != null) {
      return List.of(
          buildIndexUpsert(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey),
              JobIndexBackendSupport.KIND_GLOBAL_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(upsert.pointerKey());
    if (accountStateKey != null) {
      return List.of(
          buildIndexUpsert(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey),
              JobIndexBackendSupport.KIND_ACCOUNT_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(upsert.pointerKey());
    if (connectorStateKey != null) {
      return List.of(
          buildIndexUpsert(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
              JobIndexBackendSupport.KIND_CONNECTOR_STATE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(upsert.pointerKey());
    if (dedupeKey != null) {
      return List.of(
          buildIndexUpsert(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey),
              JobIndexBackendSupport.KIND_DEDUPE,
              upsert,
              JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY));
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index upsert key: " + upsert.pointerKey());
  }

  private List<TransactWriteItem> buildPointerDelete(ReconcileJobIndexStore.JobIndexDelete delete) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(delete.pointerKey());
    if (lookupKey != null) {
      return DynamoReconcileJobLookupCompatibility.ownedDeletes(
          table,
          lookupKey,
          delete.expectedVersion(),
          delete.expectedCanonicalPointerKey(),
          delete.expectedLookupStoragePartitionKey());
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(delete.pointerKey());
    if (canonicalKey != null) {
      return List.of(
          buildCanonicalDelete(
              JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
              JobIndexBackendSupport.canonicalSortKey(canonicalKey),
              delete));
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(delete.pointerKey());
    if (parentKey != null) {
      return List.of(
          buildReferenceDelete(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey),
              delete));
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(delete.pointerKey());
    if (connectorKey != null) {
      return List.of(
          buildReferenceDelete(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey),
              delete));
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(delete.pointerKey());
    if (globalStateKey != null) {
      return List.of(
          buildReferenceDelete(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey),
              delete));
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(delete.pointerKey());
    if (accountStateKey != null) {
      return List.of(
          buildReferenceDelete(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey),
              delete));
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(delete.pointerKey());
    if (connectorStateKey != null) {
      return List.of(
          buildReferenceDelete(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey),
              delete));
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(delete.pointerKey());
    if (dedupeKey != null) {
      return List.of(
          buildReferenceDelete(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey),
              delete));
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index delete key: " + delete.pointerKey());
  }

  private TransactWriteItem buildPointerCheck(ReconcileJobIndexStore.JobIndexCheck check) {
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(check.pointerKey());
    if (canonicalKey == null) {
      throw new IllegalArgumentException(
          "Unsupported reconcile job index version check key: " + check.pointerKey());
    }
    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":expected", AttributeValue.fromN(Long.toString(check.expectedVersion())));
    String condition = "#v = :expected";
    if (check.requireCleanupLock()) {
      names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
      values.put(":true", AttributeValue.fromBool(true));
      condition += " AND #lock = :true";
    }
    return TransactWriteItem.builder()
        .conditionCheck(
            software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY,
                        AttributeValue.fromS(
                            JobIndexBackendSupport.canonicalPartitionKey(canonicalKey)),
                        ATTR_SORT_KEY,
                        AttributeValue.fromS(
                            JobIndexBackendSupport.canonicalSortKey(canonicalKey))))
                .conditionExpression(condition)
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build())
        .build();
  }

  private List<TransactWriteItem> buildPointerCheckAbsent(String pointerKey) {
    var lookupKey = JobIndexBackendSupport.parseLookupKey(pointerKey);
    if (lookupKey != null) {
      return DynamoReconcileJobLookupCompatibility.checkAbsent(table, lookupKey);
    }
    var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(pointerKey);
    if (canonicalKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
              JobIndexBackendSupport.canonicalSortKey(canonicalKey)));
    }
    var parentKey = JobIndexBackendSupport.parseParentKey(pointerKey);
    if (parentKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.parentPartitionKey(parentKey),
              JobIndexBackendSupport.parentSortKey(parentKey)));
    }
    var connectorKey = JobIndexBackendSupport.parseConnectorKey(pointerKey);
    if (connectorKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.connectorPartitionKey(connectorKey),
              JobIndexBackendSupport.connectorSortKey(connectorKey)));
    }
    var globalStateKey = JobIndexBackendSupport.parseGlobalStateKey(pointerKey);
    if (globalStateKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.globalStatePartitionKey(globalStateKey),
              JobIndexBackendSupport.globalStateSortKey(globalStateKey)));
    }
    var accountStateKey = JobIndexBackendSupport.parseAccountStateKey(pointerKey);
    if (accountStateKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.accountStatePartitionKey(accountStateKey),
              JobIndexBackendSupport.accountStateSortKey(accountStateKey)));
    }
    var connectorStateKey = JobIndexBackendSupport.parseConnectorStateKey(pointerKey);
    if (connectorStateKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.connectorStatePartitionKey(connectorStateKey),
              JobIndexBackendSupport.connectorStateSortKey(connectorStateKey)));
    }
    var dedupeKey = JobIndexBackendSupport.parseDedupeKey(pointerKey);
    if (dedupeKey != null) {
      return List.of(
          buildCheckAbsent(
              JobIndexBackendSupport.dedupePartitionKey(dedupeKey),
              JobIndexBackendSupport.dedupeSortKey(dedupeKey)));
    }
    throw new IllegalArgumentException(
        "Unsupported reconcile job index check-absent key: " + pointerKey);
  }

  private TransactWriteItem buildCanonicalUpsert(
      JobIndexBackendSupport.CanonicalJobKey key, ReconcileJobIndexStore.JobIndexUpsert upsert) {
    if (upsert.expectedVersion() > 0L) {
      Map<String, String> names =
          Map.ofEntries(
              Map.entry("#kind", ATTR_KIND),
              Map.entry("#v", ATTR_VERSION),
              Map.entry("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY),
              Map.entry("#blob", JobIndexBackendSupport.ATTR_BLOB_URI),
              Map.entry("#account", JobIndexBackendSupport.ATTR_ACCOUNT_ID),
              Map.entry("#job", JobIndexBackendSupport.ATTR_JOB_ID),
              Map.entry("#idx", JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS),
              Map.entry("#ready", JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS),
              Map.entry("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS),
              Map.entry("#complete", JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE),
              Map.entry("#scan", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_REQUIRED),
              Map.entry("#cursor", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_CURSOR),
              Map.entry("#drained", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_SCAN_DRAINED),
              Map.entry(
                  "#legacyIdx", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS),
              Map.entry(
                  "#legacyReady", JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS));
      Map<String, AttributeValue> values = new HashMap<>();
      values.put(":kind", AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
      values.put(":expected", AttributeValue.fromN(Long.toString(upsert.expectedVersion())));
      values.put(":next", AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
      values.put(":pointer", AttributeValue.fromS(key.pointerKey()));
      values.put(":blob", AttributeValue.fromS(upsert.blobUri()));
      values.put(":account", AttributeValue.fromS(key.accountSegment()));
      values.put(":job", AttributeValue.fromS(key.jobSegment()));
      values.put(":idx", stringListValue(upsert.cleanupManifest().indexPointerKeys()));
      values.put(":ready", stringListValue(upsert.cleanupManifest().readyPointerKeys()));
      values.put(":true", AttributeValue.fromBool(true));
      return TransactWriteItem.builder()
          .update(
              Update.builder()
                  .tableName(table)
                  .key(
                      Map.of(
                          ATTR_PARTITION_KEY,
                          AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)),
                          ATTR_SORT_KEY,
                          AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key))))
                  .updateExpression(
                      "SET #kind = :kind, #v = :next, #pointer = :pointer, #blob = :blob, "
                          + "#account = :account, #job = :job, #idx = :idx, #ready = :ready, "
                          + "#complete = :true REMOVE #scan, #cursor, #drained, #legacyIdx, #legacyReady")
                  .conditionExpression("#v = :expected AND attribute_not_exists(#lock)")
                  .expressionAttributeNames(names)
                  .expressionAttributeValues(values)
                  .build())
          .build();
    }
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(
        ATTR_PARTITION_KEY,
        AttributeValue.fromS(JobIndexBackendSupport.canonicalPartitionKey(key)));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(JobIndexBackendSupport.canonicalSortKey(key)));
    item.put(ATTR_KIND, AttributeValue.fromS(JobIndexBackendSupport.KIND_CANONICAL_JOB));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(key.pointerKey()));
    item.put(JobIndexBackendSupport.ATTR_BLOB_URI, AttributeValue.fromS(upsert.blobUri()));
    item.put(JobIndexBackendSupport.ATTR_ACCOUNT_ID, AttributeValue.fromS(key.accountSegment()));
    item.put(JobIndexBackendSupport.ATTR_JOB_ID, AttributeValue.fromS(key.jobSegment()));
    item.put(JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE, AttributeValue.fromBool(true));
    putStringList(
        item,
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS,
        upsert.cleanupManifest().indexPointerKeys());
    putStringList(
        item,
        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS,
        upsert.cleanupManifest().readyPointerKeys());
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildIndexUpsert(
      String partitionKey,
      String sortKey,
      String kind,
      ReconcileJobIndexStore.JobIndexUpsert upsert,
      String referenceAttributeName) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(sortKey));
    item.put(ATTR_KIND, AttributeValue.fromS(kind));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(JobIndexBackendSupport.ATTR_POINTER_KEY, AttributeValue.fromS(upsert.pointerKey()));
    item.put(referenceAttributeName, AttributeValue.fromS(upsert.blobUri()));
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildPut(Map<String, AttributeValue> item, long expectedVersion) {
    Put.Builder put = Put.builder().tableName(table).item(item);
    if (expectedVersion == 0L) {
      put.conditionExpression("attribute_not_exists(#pk)")
          .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY));
    } else {
      put.conditionExpression("#v = :expected")
          .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
          .expressionAttributeValues(
              Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))));
    }
    return TransactWriteItem.builder().put(put.build()).build();
  }

  private TransactWriteItem buildGenericPointerUpsert(PointerStore.CasUpsert upsert) {
    GenericPointerKey key = genericPointerKey(upsert.key());
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()));
    item.put(ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey()));
    item.put(ATTR_KIND, AttributeValue.fromS(KIND_GENERIC_POINTER));
    item.put(ATTR_VERSION, AttributeValue.fromN(Long.toString(upsert.expectedVersion() + 1L)));
    item.put(ATTR_GENERIC_BLOB_URI, AttributeValue.fromS(upsert.next().getBlobUri()));
    if (upsert.next().getReferenceKind()
        != ai.floedb.floecat.common.rpc.PointerReferenceKind.PRK_UNSPECIFIED) {
      item.put(
          ATTR_GENERIC_REFERENCE_KIND,
          AttributeValue.fromS(upsert.next().getReferenceKind().name()));
    }
    return buildPut(item, upsert.expectedVersion());
  }

  private TransactWriteItem buildGenericPointerDelete(String pointerKey, long expectedVersion) {
    GenericPointerKey key = genericPointerKey(pointerKey);
    return buildDelete(key.partitionKey(), key.sortKey(), expectedVersion);
  }

  private TransactWriteItem buildGenericPointerCheck(String pointerKey, long expectedVersion) {
    GenericPointerKey key = genericPointerKey(pointerKey);
    return TransactWriteItem.builder()
        .conditionCheck(
            software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(key.partitionKey()),
                        ATTR_SORT_KEY, AttributeValue.fromS(key.sortKey())))
                .conditionExpression("#v = :expected")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(
                    Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))))
                .build())
        .build();
  }

  private TransactWriteItem buildGenericPointerCheckAbsent(String pointerKey) {
    GenericPointerKey key = genericPointerKey(pointerKey);
    return buildCheckAbsent(key.partitionKey(), key.sortKey());
  }

  private TransactWriteItem buildCheckAbsent(String partitionKey, String sortKey) {
    return TransactWriteItem.builder()
        .conditionCheck(
            software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression("attribute_not_exists(#pk)")
                .expressionAttributeNames(Map.of("#pk", ATTR_PARTITION_KEY))
                .build())
        .build();
  }

  private static String stripLeadingSlash(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }

  private static GenericPointerKey genericPointerKey(String pointerKey) {
    String key = pointerKey == null ? "" : pointerKey;
    String k = key.startsWith("/") ? key.substring(1) : key;
    String accountByIdPrefix = stripLeadingSlash(Keys.accountPointerByIdPrefix());
    String accountByNamePrefix = stripLeadingSlash(Keys.accountPointerByNamePrefix());
    String accountRootPrefix = stripLeadingSlash(Keys.accountRootPrefix());
    if (k.startsWith(accountByIdPrefix) || k.startsWith(accountByNamePrefix)) {
      return new GenericPointerKey(GENERIC_POINTER_GLOBAL_PK, k);
    }
    if (!k.startsWith(accountRootPrefix)) {
      throw new IllegalArgumentException("unexpected key: " + pointerKey);
    }
    int firstSlash = k.indexOf('/');
    int secondSlash = k.indexOf('/', firstSlash + 1);
    if (secondSlash < 0) {
      throw new IllegalArgumentException("bad key: " + pointerKey);
    }
    String accountId = k.substring(firstSlash + 1, secondSlash);
    String remainder = k.substring(secondSlash + 1);
    return new GenericPointerKey(accountRootPrefix + accountId, remainder);
  }

  private record GenericPointerKey(String partitionKey, String sortKey) {}

  private TransactWriteItem buildCanonicalDelete(
      String partitionKey, String sortKey, ReconcileJobIndexStore.JobIndexDelete delete) {
    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":expected", AttributeValue.fromN(Long.toString(delete.expectedVersion())));
    String condition = "#v = :expected";
    if (delete.requireCleanupLock()) {
      names.put("#lock", JobIndexBackendSupport.ATTR_CLEANUP_DELETE_IN_PROGRESS);
      values.put(":true", AttributeValue.fromBool(true));
      condition += " AND #lock = :true";
    }
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression(condition)
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build())
        .build();
  }

  private TransactWriteItem buildDelete(String partitionKey, String sortKey, long expectedVersion) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression("#v = :expected")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION))
                .expressionAttributeValues(
                    Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))))
                .build())
        .build();
  }

  private TransactWriteItem buildDeleteWithReference(
      String partitionKey,
      String sortKey,
      long expectedVersion,
      String referenceAttribute,
      String expectedReference) {
    return TransactWriteItem.builder()
        .delete(
            Delete.builder()
                .tableName(table)
                .key(
                    Map.of(
                        ATTR_PARTITION_KEY, AttributeValue.fromS(partitionKey),
                        ATTR_SORT_KEY, AttributeValue.fromS(sortKey)))
                .conditionExpression("#v = :expected AND #ref = :reference")
                .expressionAttributeNames(Map.of("#v", ATTR_VERSION, "#ref", referenceAttribute))
                .expressionAttributeValues(
                    Map.of(
                        ":expected", AttributeValue.fromN(Long.toString(expectedVersion)),
                        ":reference", AttributeValue.fromS(expectedReference)))
                .build())
        .build();
  }

  private TransactWriteItem buildReferenceDelete(
      String partitionKey, String sortKey, ReconcileJobIndexStore.JobIndexDelete delete) {
    if (delete.expectedCanonicalPointerKey().isBlank()) {
      return buildDelete(partitionKey, sortKey, delete.expectedVersion());
    }
    return buildDeleteWithReference(
        partitionKey,
        sortKey,
        delete.expectedVersion(),
        JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
        delete.expectedCanonicalPointerKey());
  }

  private JobIndexBackendSupport.CanonicalJobKey parseCanonicalPrefix(String prefix) {
    return JobIndexBackendSupport.parseCanonicalPrefix(prefix);
  }

  private JobIndexBackendSupport.ParentKey parseParentPrefix(String prefix) {
    return JobIndexBackendSupport.parseParentPrefix(prefix);
  }

  private JobIndexBackendSupport.ConnectorKey parseConnectorPrefix(String prefix) {
    return JobIndexBackendSupport.parseConnectorPrefix(prefix);
  }

  private JobIndexBackendSupport.GlobalStateKey parseGlobalStatePrefix(String prefix) {
    return JobIndexBackendSupport.parseGlobalStatePrefix(prefix);
  }

  private JobIndexBackendSupport.AccountStateKey parseAccountStatePrefix(String prefix) {
    return JobIndexBackendSupport.parseAccountStatePrefix(prefix);
  }

  private JobIndexBackendSupport.ConnectorStateKey parseConnectorStatePrefix(String prefix) {
    return JobIndexBackendSupport.parseConnectorStatePrefix(prefix);
  }

  private static String stringAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    return value == null || value.s() == null ? "" : value.s();
  }

  private static boolean boolAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    return value != null && Boolean.TRUE.equals(value.bool());
  }

  private static long longAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    if (value == null || value.n() == null) {
      return 0L;
    }
    try {
      return Long.parseLong(value.n());
    } catch (NumberFormatException ignored) {
      return 0L;
    }
  }

  private static int nonNegativeIntAttr(Map<String, AttributeValue> item, String name) {
    long value = longAttr(item, name);
    if (value <= 0L) {
      return 0;
    }
    return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }

  private static List<String> stringListAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    if (value == null || !value.hasL()) {
      return List.of();
    }
    List<String> values = new ArrayList<>();
    for (AttributeValue entry : value.l()) {
      if (entry != null && entry.s() != null && !entry.s().isBlank()) {
        values.add(entry.s());
      }
    }
    return List.copyOf(values);
  }

  private static List<String> stringSetAttr(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    if (value == null) {
      return List.of();
    }
    if (value.hasSs()) {
      return value.ss().stream().filter(entry -> entry != null && !entry.isBlank()).toList();
    }
    // Accept the short-lived list representation so an interrupted pre-release migration can be
    // promoted instead of forcing a table scan.
    return stringListAttr(item, name);
  }

  private static AttributeValue stringListValue(List<String> values) {
    return AttributeValue.fromL(
        values == null
            ? List.of()
            : values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(AttributeValue::fromS)
                .toList());
  }

  private static ReconcileJobIndexCleanupManifest cleanupManifest(
      Map<String, AttributeValue> item) {
    return new ReconcileJobIndexCleanupManifest(
        stringListAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS),
        stringListAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS));
  }

  private static ReconcileJobIndexCleanupManifest legacyCleanupManifest(
      Map<String, AttributeValue> item) {
    return new ReconcileJobIndexCleanupManifest(
        stringSetAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_INDEX_POINTER_KEYS),
        stringSetAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_LEGACY_READY_POINTER_KEYS));
  }

  private boolean manifestWithinMigrationLimit(ReconcileJobIndexCleanupManifest manifest) {
    long bytes = 0L;
    for (String pointerKey : manifest.indexPointerKeys()) {
      bytes += pointerKey.getBytes(StandardCharsets.UTF_8).length + 32L;
      if (bytes > Math.max(1, legacyCleanupManifestMaxBytes)) {
        return false;
      }
    }
    for (String pointerKey : manifest.readyPointerKeys()) {
      bytes += pointerKey.getBytes(StandardCharsets.UTF_8).length + 32L;
      if (bytes > Math.max(1, legacyCleanupManifestMaxBytes)) {
        return false;
      }
    }
    return true;
  }

  private static int boundedPageSize(int requested, int hardMaximum) {
    return Math.min(Math.max(1, requested), hardMaximum);
  }

  private static void putStringList(
      Map<String, AttributeValue> item, String name, List<String> values) {
    if (values == null || values.isEmpty()) {
      return;
    }
    List<AttributeValue> attrs = new ArrayList<>();
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        attrs.add(AttributeValue.fromS(value));
      }
    }
    if (!attrs.isEmpty()) {
      item.put(name, AttributeValue.fromL(attrs));
    }
  }
}
