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

package ai.floedb.floecat.service.query.catalog.testsupport;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.resolver.QueryInputResolver;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.UnaryOperator;

public final class UserObjectBundleTestSupport {

  private UserObjectBundleTestSupport() {}

  public static List<ai.floedb.floecat.query.rpc.SchemaColumn> schemaFor(String name) {
    return List.of(
        ai.floedb.floecat.query.rpc.SchemaColumn.newBuilder()
            .setName(name)
            .setNullable(true)
            .build());
  }

  public static final class FakeCatalogOverlay implements CatalogOverlay {
    private final Map<String, SimpleGraphNode> nodes = new HashMap<>();
    private final Map<String, List<ai.floedb.floecat.query.rpc.SchemaColumn>> schemas =
        new HashMap<>();
    private final Map<String, NameRef> names = new HashMap<>();
    private final Map<String, CatalogNode> catalogs = new HashMap<>();
    private final Set<String> hidden = new HashSet<>();

    public void clear() {
      nodes.clear();
      schemas.clear();
      names.clear();
      catalogs.clear();
      hidden.clear();
    }

    public void registerTable(
        ResourceId id, List<ai.floedb.floecat.query.rpc.SchemaColumn> schema, NameRef name) {
      registerTable(id, schema, name, GraphNodeOrigin.USER);
    }

    public void registerTable(
        ResourceId id,
        List<ai.floedb.floecat.query.rpc.SchemaColumn> schema,
        NameRef name,
        GraphNodeOrigin origin) {
      nodes.put(id.getId(), new SimpleGraphNode(id, origin));
      schemas.put(id.getId(), schema);
      names.put(id.getId(), name);
    }

    public void registerCatalog(ResourceId id, String displayName) {
      CatalogNode node =
          new CatalogNode(
              id,
              0,
              Instant.EPOCH,
              displayName,
              Map.of(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Map.of());
      catalogs.put(id.getId(), node);
    }

    public void hideNode(ResourceId id) {
      hidden.add(id.getId());
    }

    private UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException("Not needed for tests");
    }

    @Override
    public Optional<GraphNode> resolve(ResourceId id) {
      if (hidden.contains(id.getId())) {
        return Optional.empty();
      }
      return Optional.ofNullable(nodes.get(id.getId()));
    }

    @Override
    public List<GraphNode> listRelations(ResourceId catalogId) {
      throw unsupported();
    }

    @Override
    public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
      throw unsupported();
    }

    @Override
    public List<GraphNode> listRelationsInNamespace(ResourceId catalogId, ResourceId namespaceId) {
      throw unsupported();
    }

    @Override
    public List<FunctionNode> listFunctions(ResourceId catalogId, ResourceId namespaceId) {
      throw unsupported();
    }

    @Override
    public List<TypeNode> listTypes(ResourceId catalogId) {
      throw unsupported();
    }

    @Override
    public Optional<ResourceId> resolveCatalog(String correlationId, String name) {
      throw unsupported();
    }

    @Override
    public Optional<ResourceId> resolveNamespace(String correlationId, NameRef ref) {
      throw unsupported();
    }

    @Override
    public Optional<ResourceId> resolveTable(String correlationId, NameRef ref) {
      throw unsupported();
    }

    @Override
    public Optional<ResourceId> resolveView(String correlationId, NameRef ref) {
      throw unsupported();
    }

    @Override
    public Optional<ResourceId> resolveName(String correlationId, NameRef ref) {
      return names.entrySet().stream()
          .filter(entry -> entry.getValue().equals(ref))
          .map(entry -> nodes.get(entry.getKey()).id())
          .findFirst();
    }

    @Override
    public SnapshotPin snapshotPinFor(
        String correlationId,
        ResourceId tableId,
        ai.floedb.floecat.common.rpc.SnapshotRef override,
        Optional<Timestamp> asOfDefault) {
      throw unsupported();
    }

    @Override
    public ResolveResult batchResolveTables(
        String correlationId, List<NameRef> items, int limit, String token) {
      throw unsupported();
    }

    @Override
    public ResolveResult listTablesByPrefix(
        String correlationId, NameRef prefix, int limit, String token) {
      throw unsupported();
    }

    @Override
    public ResolveResult batchResolveViews(
        String correlationId, List<NameRef> items, int limit, String token) {
      throw unsupported();
    }

    @Override
    public ResolveResult listViewsByPrefix(
        String correlationId, NameRef prefix, int limit, String token) {
      throw unsupported();
    }

    @Override
    public Optional<NameRef> namespaceName(ResourceId id) {
      throw unsupported();
    }

    @Override
    public Optional<NameRef> tableName(ResourceId id) {
      return Optional.ofNullable(names.get(id.getId()));
    }

    @Override
    public Optional<NameRef> viewName(ResourceId id) {
      throw unsupported();
    }

    @Override
    public Optional<CatalogNode> catalog(ResourceId id) {
      return Optional.ofNullable(catalogs.get(id.getId()));
    }

    @Override
    public SchemaResolution schemaFor(
        String correlationId,
        ResourceId tableId,
        ai.floedb.floecat.common.rpc.SnapshotRef snapshot) {
      throw unsupported();
    }

    @Override
    public List<ai.floedb.floecat.query.rpc.SchemaColumn> tableSchema(ResourceId tableId) {
      return schemas.getOrDefault(tableId.getId(), List.of());
    }
  }

  private static final class SimpleGraphNode implements GraphNode {
    private final ResourceId id;
    private final GraphNodeOrigin origin;

    private SimpleGraphNode(ResourceId id, GraphNodeOrigin origin) {
      this.id = id;
      this.origin = origin;
    }

    @Override
    public ResourceId id() {
      return id;
    }

    @Override
    public long version() {
      return 1;
    }

    @Override
    public String displayName() {
      return id.getId();
    }

    @Override
    public Instant metadataUpdatedAt() {
      return Instant.EPOCH;
    }

    @Override
    public GraphNodeKind kind() {
      return GraphNodeKind.TABLE;
    }

    @Override
    public GraphNodeOrigin origin() {
      return origin;
    }

    @Override
    public Map<EngineHintKey, EngineHint> engineHints() {
      return Map.of();
    }
  }

  public static final class TestQueryInputResolver extends QueryInputResolver {
    private final List<List<QueryInput>> calls = new ArrayList<>();
    private int nextSnapshotId = 1;

    public TestQueryInputResolver() {}

    public TestQueryInputResolver(int nextSnapshotId) {
      this.nextSnapshotId = nextSnapshotId;
    }

    public List<List<QueryInput>> recordedInputs() {
      return new ArrayList<>(calls);
    }

    @Override
    public ResolutionResult resolveInputs(
        String correlationId, List<QueryInput> inputs, Optional<Timestamp> asOfDefault) {
      calls.add(new ArrayList<>(inputs));
      ResourceId rid = inputs.get(0).getTableId();
      SnapshotPin pin =
          SnapshotPin.newBuilder().setTableId(rid).setSnapshotId(nextSnapshotId++).build();
      return new ResolutionResult(
          List.of(rid), SnapshotSet.newBuilder().addPins(pin).build(), null);
    }
  }

  public static final class TestQueryContextStore implements QueryContextStore {
    private final Map<String, QueryContext> contexts = new HashMap<>();
    private final List<QueryContext> updates = new ArrayList<>();

    public void seed(QueryContext ctx) {
      contexts.put(ctx.getQueryId(), ctx);
    }

    public int updateCount() {
      return updates.size();
    }

    @Override
    public Optional<QueryContext> get(String queryId) {
      return Optional.ofNullable(contexts.get(queryId));
    }

    @Override
    public void put(QueryContext ctx) {
      contexts.put(ctx.getQueryId(), ctx);
    }

    @Override
    public Optional<QueryContext> extendLease(String queryId, long requestedExpiresAtMs) {
      return Optional.empty();
    }

    @Override
    public Optional<QueryContext> end(String queryId, boolean commit) {
      return Optional.empty();
    }

    @Override
    public boolean delete(String queryId) {
      return contexts.remove(queryId) != null;
    }

    @Override
    public long size() {
      return contexts.size();
    }

    @Override
    public void replace(QueryContext ctx) {
      contexts.put(ctx.getQueryId(), ctx);
    }

    @Override
    public Optional<QueryContext> update(String queryId, UnaryOperator<QueryContext> fn) {
      QueryContext current = contexts.get(queryId);
      if (current == null) {
        return Optional.empty();
      }
      QueryContext updated = fn.apply(current);
      if (updated == null || updated == current) {
        return Optional.of(current);
      }
      contexts.put(queryId, updated);
      updates.add(updated);
      return Optional.of(updated);
    }

    @Override
    public void close() {}
  }

  public static class CollectingSubscriber implements Subscriber<UserObjectsBundleChunk> {
    private final List<UserObjectsBundleChunk> events = new ArrayList<>();
    protected Subscription subscription;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    @Override
    public void onSubscribe(Subscription s) {
      this.subscription = s;
      s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(UserObjectsBundleChunk chunk) {
      events.add(chunk);
    }

    @Override
    public void onError(Throwable t) {
      completion.completeExceptionally(t);
    }

    @Override
    public void onComplete() {
      completion.complete(null);
    }

    public List<UserObjectsBundleChunk> items() {
      return events;
    }

    public void await() {
      completion.join();
    }

    protected final void completeSuccessfully() {
      completion.complete(null);
    }
  }

  public static final class CancellingSubscriber extends CollectingSubscriber {
    private boolean cancelled;

    @Override
    public void onNext(UserObjectsBundleChunk chunk) {
      super.onNext(chunk);
      if (!cancelled && chunk.hasResolutions()) {
        cancelled = true;
        if (subscription != null) {
          subscription.cancel();
        }
        completeSuccessfully();
      }
    }
  }
}
