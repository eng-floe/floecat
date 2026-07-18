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
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileJobIndexBackend implements ReconcileJobIndexBackend {
  private static final String KIND_GENERIC_POINTER = "Pointer";
  private static final String GENERIC_POINTER_GLOBAL_PK = "_ACCOUNT_DIR";
  private static final String ATTR_GENERIC_BLOB_URI = "blob_uri";
  private static final String ATTR_GENERIC_REFERENCE_KIND = "reference_kind";
  private static final String LEGACY_CLEANUP_MARKER_PARTITION = "reconcile-job-maintenance";
  private static final String LEGACY_CLEANUP_MARKER_SORT = "legacy-cleanup-manifest-v1";
  private static final String KIND_LEGACY_CLEANUP_MARKER = "ReconcileJobLegacyCleanupMigration";
  private static final String LEGACY_LOOKUP_MARKER_SORT = "legacy-lookup-v1";
  private static final String PHYSICAL_SORT_TOKEN_PREFIX = "dynamo-sort:";
  private static final String KIND_LEGACY_LOOKUP_MARKER = "ReconcileJobLegacyLookupMigration";

  @Inject Instance<DynamoDbClientManager> dynamoDbClientManager;
  private final RefreshingDynamoCaller dynamoCaller = new RefreshingDynamoCaller();

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  private volatile boolean legacyCleanupMigrationMarkedComplete;
  private volatile boolean legacyLookupMigrationMarkedComplete;

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
      Optional<JobIndexEntrySnapshot> legacy = loadLookupPointer(legacyKey);
      if (legacy.isEmpty()) {
        return Optional.empty();
      }
      LegacyLookupMigrationOutcome migration = migrateLegacyLookup(lookupKey, legacy.get());
      if (migration == LegacyLookupMigrationOutcome.MIGRATED) {
        return Optional.of(
            new JobIndexEntrySnapshot(
                legacy.get().pointerKey(),
                legacy.get().blobUri(),
                legacy.get().version(),
                currentKey.partitionKey()));
      }
      if (migration == LegacyLookupMigrationOutcome.CONFLICT_RESOLVED) {
        return loadLookupPointer(currentKey);
      }
      return loadLookupPointer(currentKey).or(() -> legacy);
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
    if (legacyLookupMigrationMarkedComplete()) {
      return new LegacyLookupMigrationPage(0, 0, 0, 0, "");
    }
    JobIndexQueryPage page =
        listIndexPointers(
            JobIndexBackendSupport.legacyLookupPartitionKey(),
            pageToken,
            Math.max(1, limit),
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
    if (legacyLookupMigrationMarkedComplete()) {
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
    if (!boolAttr(response.item(), JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE)
        && !legacyCleanupMigrationMarkedComplete()) {
      return ReconcileJobIndexCleanupManifest.EMPTY;
    }
    return new ReconcileJobIndexCleanupManifest(
        stringListAttr(response.item(), JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS),
        stringListAttr(response.item(), JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS));
  }

  @Override
  public LegacyCleanupMigrationPage migrateLegacyCleanupManifests(int limit, String pageToken) {
    if (legacyCleanupMigrationMarkedComplete()) {
      return new LegacyCleanupMigrationPage(0, 0, 0, 0, "");
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
        Map.of(
            "#kind", ATTR_KIND,
            "#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY,
            "#canonical", JobIndexBackendSupport.ATTR_CANONICAL_POINTER_KEY,
            "#blob", JobIndexBackendSupport.ATTR_BLOB_URI,
            "#ready", DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY);
    ScanRequest.Builder scan =
        ScanRequest.builder()
            .tableName(table)
            .consistentRead(true)
            .limit(Math.max(1, limit))
            .expressionAttributeNames(names)
            .filterExpression(
                "#kind IN ("
                    + String.join(", ", kindTokens)
                    + ") AND (attribute_exists(#canonical) OR attribute_exists(#blob))")
            .projectionExpression("#kind, #pointer, #canonical, #blob, #ready")
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
    int conflicted = 0;
    for (var item : response.items()) {
      String canonicalPointerKey = canonicalPointerForCleanupItem(item);
      if (blank(canonicalPointerKey)
          || JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey) == null) {
        conflicted++;
        continue;
      }
      LegacyCleanupManifestBuilder manifest =
          manifests.computeIfAbsent(
              canonicalPointerKey, ignored -> new LegacyCleanupManifestBuilder());
      var canonicalKey = JobIndexBackendSupport.parseCanonicalJobKey(canonicalPointerKey);
      manifest.indexKeys.add(
          Keys.reconcileJobLookupPointerByIdPrefix() + canonicalKey.jobSegment());
      String readyKey = stringAttr(item, DynamoReconcileReadyQueueBackend.ATTR_READY_POINTER_KEY);
      if (!blank(readyKey)) {
        manifest.readyKeys.add(readyKey);
        continue;
      }
      String pointerKey = stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY);
      if (!blank(pointerKey) && !canonicalPointerKey.equals(pointerKey)) {
        manifest.indexKeys.add(pointerKey);
      }
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
        response.scannedCount(), updated, conflicted, retryable, nextToken);
  }

  @Override
  public boolean completeLegacyCleanupMigration() {
    if (legacyCleanupMigrationMarkedComplete()) {
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
      return true;
    } catch (RuntimeException ignored) {
      return false;
    }
  }

  @Override
  public boolean legacyCleanupMigrationComplete() {
    return legacyCleanupMigrationMarkedComplete();
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
    if (boolAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE)) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }

    List<String> existingIndexKeys =
        stringListAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS);
    List<String> existingReadyKeys =
        stringListAttr(item, JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS);
    ReconcileJobIndexCleanupManifest merged =
        new ReconcileJobIndexCleanupManifest(
            concat(existingIndexKeys, discovered.indexPointerKeys()),
            concat(existingReadyKeys, discovered.readyPointerKeys()));
    if (merged.indexPointerKeys().equals(existingIndexKeys)
        && merged.readyPointerKeys().equals(existingReadyKeys)) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }

    Map<String, String> names = new HashMap<>();
    names.put("#v", ATTR_VERSION);
    names.put("#pointer", JobIndexBackendSupport.ATTR_POINTER_KEY);
    names.put("#idx", JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS);
    names.put("#ready", JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS);
    names.put("#complete", JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE);
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":v", AttributeValue.fromN(Long.toString(longAttr(item, ATTR_VERSION))));
    values.put(":canonical", AttributeValue.fromS(canonicalPointerKey));
    values.put(":idx", stringListValue(merged.indexPointerKeys()));
    values.put(":ready", stringListValue(merged.readyPointerKeys()));
    StringBuilder condition =
        new StringBuilder("#v = :v AND #pointer = :canonical AND attribute_not_exists(#complete)");
    appendExpectedListCondition(
        condition,
        values,
        item,
        "#idx",
        ":oldIdx",
        JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS);
    appendExpectedListCondition(
        condition,
        values,
        item,
        "#ready",
        ":oldReady",
        JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS);
    try {
      dynamoCaller.callVoid(
          dynamoDbClientManager,
          client ->
              client.updateItem(
                  UpdateItemRequest.builder()
                      .tableName(table)
                      .key(dynamoKey)
                      .updateExpression("SET #idx = :idx, #ready = :ready")
                      .conditionExpression(condition.toString())
                      .expressionAttributeNames(names)
                      .expressionAttributeValues(values)
                      .build()));
      return LegacyCleanupMergeOutcome.UPDATED;
    } catch (ConditionalCheckFailedException ignored) {
      return classifyLegacyCleanupConditionalConflict(dynamoKey, canonicalPointerKey, merged);
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
        return LegacyCleanupMergeOutcome.CONFLICTED;
      }
      current = response.item();
    } catch (RuntimeException ignored) {
      return LegacyCleanupMergeOutcome.RETRYABLE;
    }
    if (!canonicalPointerKey.equals(stringAttr(current, JobIndexBackendSupport.ATTR_POINTER_KEY))) {
      return LegacyCleanupMergeOutcome.CONFLICTED;
    }
    if (boolAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_MANIFEST_COMPLETE)) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }
    List<String> currentIndexKeys =
        stringListAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_INDEX_POINTER_KEYS);
    List<String> currentReadyKeys =
        stringListAttr(current, JobIndexBackendSupport.ATTR_CLEANUP_READY_POINTER_KEYS);
    if (currentIndexKeys.containsAll(expectedManifest.indexPointerKeys())
        && currentReadyKeys.containsAll(expectedManifest.readyPointerKeys())) {
      return LegacyCleanupMergeOutcome.UNCHANGED;
    }
    return LegacyCleanupMergeOutcome.RETRYABLE;
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

  private static void appendExpectedListCondition(
      StringBuilder condition,
      Map<String, AttributeValue> values,
      Map<String, AttributeValue> item,
      String nameToken,
      String valueToken,
      String attributeName) {
    AttributeValue existing = item.get(attributeName);
    if (existing == null) {
      condition.append(" AND attribute_not_exists(").append(nameToken).append(')');
    } else {
      condition.append(" AND ").append(nameToken).append(" = ").append(valueToken);
      values.put(valueToken, existing);
    }
  }

  private static AttributeValue stringListValue(List<String> values) {
    return AttributeValue.fromL(values.stream().map(AttributeValue::fromS).toList());
  }

  private static List<String> concat(List<String> left, List<String> right) {
    List<String> values = new ArrayList<>(left.size() + right.size());
    values.addAll(left);
    values.addAll(right);
    return values;
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
            longAttr(response.item(), ATTR_VERSION)));
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
      pointers.add(
          new JobIndexEntrySnapshot(
              stringAttr(item, JobIndexBackendSupport.ATTR_POINTER_KEY),
              stringAttr(item, referenceAttributeName),
              longAttr(item, ATTR_VERSION)));
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
          buildDelete(
              JobIndexBackendSupport.canonicalPartitionKey(canonicalKey),
              JobIndexBackendSupport.canonicalSortKey(canonicalKey),
              delete.expectedVersion()));
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
