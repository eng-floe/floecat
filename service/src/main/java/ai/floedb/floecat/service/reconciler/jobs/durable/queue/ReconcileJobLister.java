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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJobPage;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.projection.ReconcileJobProjector;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobLister {
  private static final Logger LOG = Logger.getLogger(ReconcileJobLister.class);

  private static final int LIST_SCAN_MAX_PAGES = 1_000;
  private static final String LIST_TOKEN_V1_PREFIX = "v1:";
  private static final String STATE_LIST_TOKEN_V1_PREFIX = "v1s:";

  private PointerStore pointerStore;
  private ReconcileJobProjector projector;
  private Function<Pointer, Optional<StoredReconcileJob>> readRecord;
  private Function<Pointer, Optional<StoredReconcileJob>> readCurrentRecordFromIndexPointer;
  private BiFunction<Pointer, Predicate<StoredReconcileJob>, Optional<StoredReconcileJob>>
      readCurrentRecordFromStateIndexPointer;
  private BiPredicate<String, String> canonicalJobPointerMatcher;

  public void bind(
      PointerStore pointerStore,
      ReconcileJobProjector projector,
      Function<Pointer, Optional<StoredReconcileJob>> readRecord,
      Function<Pointer, Optional<StoredReconcileJob>> readCurrentRecordFromIndexPointer,
      BiFunction<Pointer, Predicate<StoredReconcileJob>, Optional<StoredReconcileJob>>
          readCurrentRecordFromStateIndexPointer,
      BiPredicate<String, String> canonicalJobPointerMatcher) {
    this.pointerStore = pointerStore;
    this.projector = projector;
    this.readRecord = readRecord;
    this.readCurrentRecordFromIndexPointer = readCurrentRecordFromIndexPointer;
    this.readCurrentRecordFromStateIndexPointer = readCurrentRecordFromStateIndexPointer;
    this.canonicalJobPointerMatcher = canonicalJobPointerMatcher;
  }

  public ReconcileJobPage list(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    Set<String> normalizedStates = normalizeStateFilter(states);
    if (!normalizedStates.isEmpty()) {
      List<String> orderedStates = orderedStateFilter(normalizedStates);
      if (connectorId != null && !connectorId.isBlank()) {
        return listByConnectorStateIndexes(
            accountId, pageSize, pageToken, connectorId, orderedStates);
      }
      return listByAccountStateIndexes(accountId, pageSize, pageToken, orderedStates);
    }
    if (connectorId != null && !connectorId.isBlank()) {
      return listByConnectorIndex(accountId, pageSize, pageToken, connectorId, normalizedStates);
    }
    return listAccountWide(accountId, pageSize, pageToken, normalizedStates);
  }

  public ReconcileJobPage childJobsPage(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return new ReconcileJobPage(List.of(), "");
    }
    int limit = Math.max(1, pageSize);
    List<ReconcileJob> out = new ArrayList<>();
    String token = pageToken == null ? "" : pageToken;
    String prefix = Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId);
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, limit, token, next);
    for (Pointer ptr : pointers) {
      var rec = readCurrentRecordFromIndexPointer.apply(ptr);
      rec.ifPresent(stored -> out.add(projector.toPublicJob(stored, true)));
    }
    return new ReconcileJobPage(out, next.toString());
  }

  private ReconcileJobPage listAccountWide(
      String accountId, int pageSize, String pageToken, Set<String> states) {
    long startedAtMs = System.currentTimeMillis();
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<ReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    int pointerCount = 0;
    int recordCount = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileJobPointerByIdPrefix(accountId), Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      pointerCount += pointers.size();
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        if (!canonicalJobPointerMatcher.test(accountId, ptr.getKey())) {
          continue;
        }
        var rec = readRecord.apply(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        recordCount++;
        var job = projector.toPublicJobSummary(rec.get());
        if (states != null && !states.isEmpty() && !states.contains(job.state)) {
          continue;
        }
        out.add(job);
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          if (!hasMore) {
            nextToken = "";
          } else if (i + 1 < pointers.size()) {
            nextToken = encodeListCursor(token, i + 1);
          } else {
            nextToken = next.toString();
          }
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
      if (nextToken.isBlank()) {
        break;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Account-wide reconcile job list hit page cap accountId=%s out=%d",
            accountId, out.size());
        break;
      }
      token = nextToken;
    }
    LOG.debugf(
        "list total_ms=%d pointer_count=%d record_count=%d contribution_count=%d mode=account accountId=%s returned=%d",
        System.currentTimeMillis() - startedAtMs,
        pointerCount,
        recordCount,
        0,
        accountId,
        out.size());
    return new ReconcileJobPage(out, nextToken);
  }

  private ReconcileJobPage listByConnectorIndex(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    long startedAtMs = System.currentTimeMillis();
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    String prefix = Keys.reconcileJobByConnectorPointerPrefix(accountId, connectorId);
    List<ReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    int pointerCount = 0;
    int recordCount = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      pointerCount += pointers.size();
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        var rec = readCurrentRecordFromIndexPointer.apply(ptr);
        if (rec.isEmpty()) {
          continue;
        }
        recordCount++;
        StoredReconcileJob stored = rec.get();
        if (!connectorId.equals(stored.connectorId)) {
          continue;
        }
        var job = projector.toPublicJobSummary(stored);
        if (states != null && !states.isEmpty() && !states.contains(job.state)) {
          continue;
        }
        out.add(job);
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          if (!hasMore) {
            nextToken = "";
          } else if (i + 1 < pointers.size()) {
            nextToken = encodeListCursor(token, i + 1);
          } else {
            nextToken = next.toString();
          }
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
      if (nextToken.isBlank()) {
        break;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "Connector reconcile job list hit page cap accountId=%s connectorId=%s out=%d",
            accountId, connectorId, out.size());
        break;
      }
      token = nextToken;
    }
    LOG.debugf(
        "list total_ms=%d pointer_count=%d record_count=%d contribution_count=%d mode=connector accountId=%s connectorId=%s returned=%d",
        System.currentTimeMillis() - startedAtMs,
        pointerCount,
        recordCount,
        0,
        accountId,
        connectorId,
        out.size());
    return new ReconcileJobPage(out, nextToken);
  }

  private ReconcileJobPage listByAccountStateIndex(
      String accountId, int pageSize, String pageToken, String state) {
    if (blank(accountId) || blank(state)) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStatePrefix(
        Keys.reconcileJobByAccountStatePointerPrefix(accountId, state),
        pageSize,
        pageToken,
        stored -> accountId.equals(stored.accountId) && state.equals(stored.state));
  }

  private ReconcileJobPage listByAccountStateIndexes(
      String accountId, int pageSize, String pageToken, List<String> states) {
    if (blank(accountId) || states == null || states.isEmpty()) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStateIndexes(
        pageSize,
        pageToken,
        states,
        request ->
            listByAccountStateIndex(
                accountId, request.pageSize(), request.pageToken(), request.state()));
  }

  private ReconcileJobPage listByConnectorStateIndex(
      String accountId, int pageSize, String pageToken, String connectorId, String state) {
    if (blank(accountId) || blank(connectorId) || blank(state)) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStatePrefix(
        Keys.reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state),
        pageSize,
        pageToken,
        stored ->
            accountId.equals(stored.accountId)
                && connectorId.equals(stored.connectorId)
                && state.equals(stored.state));
  }

  private ReconcileJobPage listByConnectorStateIndexes(
      String accountId, int pageSize, String pageToken, String connectorId, List<String> states) {
    if (blank(accountId) || blank(connectorId) || states == null || states.isEmpty()) {
      return new ReconcileJobPage(List.of(), "");
    }
    return listByStateIndexes(
        pageSize,
        pageToken,
        states,
        request ->
            listByConnectorStateIndex(
                accountId, request.pageSize(), request.pageToken(), connectorId, request.state()));
  }

  private ReconcileJobPage listByStateIndexes(
      int pageSize,
      String pageToken,
      List<String> states,
      Function<StateListRequest, ReconcileJobPage> fetchPage) {
    int limit = Math.max(1, pageSize);
    StateListCursor cursor = decodeStateListCursor(pageToken);
    if (states == null || states.isEmpty() || cursor.stateIndex() >= states.size()) {
      return new ReconcileJobPage(List.of(), "");
    }

    List<ReconcileJob> out = new ArrayList<>(limit);
    int stateIndex = Math.max(0, cursor.stateIndex());
    String nestedPageToken = cursor.pageToken();

    while (stateIndex < states.size() && out.size() < limit) {
      ReconcileJobPage page =
          fetchPage.apply(
              new StateListRequest(
                  states.get(stateIndex), Math.max(1, limit - out.size()), nestedPageToken));

      if (page != null && page.jobs != null && !page.jobs.isEmpty()) {
        out.addAll(page.jobs);
      }

      String nextNestedToken = page == null ? "" : page.nextPageToken;
      boolean currentStateExhausted = nextNestedToken == null || nextNestedToken.isBlank();

      if (out.size() >= limit) {
        if (!currentStateExhausted) {
          return new ReconcileJobPage(out, encodeStateListCursor(stateIndex, nextNestedToken));
        }
        if (stateIndex + 1 < states.size()) {
          return new ReconcileJobPage(out, encodeStateListCursor(stateIndex + 1, ""));
        }
        return new ReconcileJobPage(out, "");
      }

      if (!currentStateExhausted) {
        nestedPageToken = nextNestedToken;
        continue;
      }

      stateIndex++;
      nestedPageToken = "";
    }

    return new ReconcileJobPage(out, "");
  }

  private ReconcileJobPage listByStatePrefix(
      String prefix, int pageSize, String pageToken, Predicate<StoredReconcileJob> filter) {
    long startedAtMs = System.currentTimeMillis();
    int limit = Math.max(1, pageSize);
    ListCursor cursor = decodeListCursor(pageToken);
    String token = cursor.storeToken();
    int skip = cursor.skip();
    List<ReconcileJob> out = new ArrayList<>(limit);
    String nextToken = "";
    int pages = 0;
    int pointerCount = 0;
    int recordCount = 0;
    while (out.size() < limit) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, Math.max(limit * 2, 64), token, next);
      if (pointers.isEmpty()) {
        break;
      }
      pointerCount += pointers.size();
      int startIndex = Math.min(skip, pointers.size());
      skip = 0;
      for (int i = startIndex; i < pointers.size(); i++) {
        Pointer ptr = pointers.get(i);
        var rec = readCurrentRecordFromStateIndexPointer.apply(ptr, filter);
        if (rec.isEmpty()) {
          continue;
        }
        recordCount++;
        out.add(projector.toPublicJobSummary(rec.get()));
        if (out.size() >= limit) {
          boolean hasMore = i + 1 < pointers.size() || next.length() > 0;
          if (!hasMore) {
            nextToken = "";
          } else if (i + 1 < pointers.size()) {
            nextToken = encodeListCursor(token, i + 1);
          } else {
            nextToken = next.toString();
          }
          break;
        }
      }
      if (out.size() >= limit) {
        break;
      }
      nextToken = next.toString();
      if (nextToken.isBlank()) {
        break;
      }
      pages++;
      if (pages >= LIST_SCAN_MAX_PAGES) {
        LOG.warnf(
            "State-index reconcile job list hit page cap prefix=%s out=%d", prefix, out.size());
        break;
      }
      token = nextToken;
    }
    LOG.debugf(
        "list total_ms=%d pointer_count=%d record_count=%d contribution_count=%d mode=state prefix=%s returned=%d",
        System.currentTimeMillis() - startedAtMs, pointerCount, recordCount, 0, prefix, out.size());
    return new ReconcileJobPage(out, nextToken);
  }

  private static ListCursor decodeListCursor(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return new ListCursor("", 0);
    }
    if (!pageToken.startsWith(LIST_TOKEN_V1_PREFIX)) {
      return new ListCursor(pageToken, 0);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder().decode(pageToken.substring(LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new ListCursor(decoded, 0);
      }
      String storeToken = decoded.substring(0, separator);
      int skip = Integer.parseInt(decoded.substring(separator + 1));
      return new ListCursor(storeToken, Math.max(0, skip));
    } catch (RuntimeException e) {
      return new ListCursor(pageToken, 0);
    }
  }

  private static String encodeListCursor(String storeToken, int skip) {
    if (skip <= 0) {
      return storeToken == null ? "" : storeToken;
    }
    String payload = (storeToken == null ? "" : storeToken) + "\n" + skip;
    return LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static StateListCursor decodeStateListCursor(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return new StateListCursor(0, "");
    }
    if (!pageToken.startsWith(STATE_LIST_TOKEN_V1_PREFIX)) {
      return new StateListCursor(0, pageToken);
    }
    try {
      String decoded =
          new String(
              Base64.getUrlDecoder()
                  .decode(pageToken.substring(STATE_LIST_TOKEN_V1_PREFIX.length())),
              StandardCharsets.UTF_8);
      int separator = decoded.indexOf('\n');
      if (separator < 0) {
        return new StateListCursor(Math.max(0, Integer.parseInt(decoded)), "");
      }
      int stateIndex = Integer.parseInt(decoded.substring(0, separator));
      String nestedPageToken = decoded.substring(separator + 1);
      return new StateListCursor(Math.max(0, stateIndex), nestedPageToken);
    } catch (RuntimeException e) {
      return new StateListCursor(0, "");
    }
  }

  private static String encodeStateListCursor(int stateIndex, String nestedPageToken) {
    if (stateIndex < 0) {
      return "";
    }
    String payload = stateIndex + "\n" + (nestedPageToken == null ? "" : nestedPageToken);
    return STATE_LIST_TOKEN_V1_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
  }

  private static Set<String> normalizeStateFilter(Set<String> states) {
    if (states == null || states.isEmpty()) {
      return Set.of();
    }
    return states.stream()
        .filter(state -> state != null && !state.isBlank())
        .map(String::trim)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static List<String> orderedStateFilter(Set<String> states) {
    if (states == null || states.isEmpty()) {
      return List.of();
    }
    return states.stream().sorted().toList();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private record ListCursor(String storeToken, int skip) {}

  private record StateListCursor(int stateIndex, String pageToken) {}

  private record StateListRequest(String state, int pageSize, String pageToken) {}
}
