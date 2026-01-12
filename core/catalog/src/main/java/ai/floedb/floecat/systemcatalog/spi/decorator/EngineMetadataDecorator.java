package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.systemcatalog.util.EngineContext;

/**
 * Engine metadata decoration hook exposed by {@link
 * ai.floedb.floecat.service.query.catalog.CatalogBundleService}.
 *
 * <p>Implementations are invoked during bundle construction and receive normalized engine kind +
 * version. Decoration remains optional and is not part of scanner implementations.
 */
public interface EngineMetadataDecorator {

  default void decorateNamespace(EngineContext ctx, NamespaceDecoration ns) {}

  default void decorateRelation(EngineContext ctx, RelationDecoration rel) {}

  default void decorateColumn(EngineContext ctx, ColumnDecoration col) {}

  default void decorateView(EngineContext ctx, ViewDecoration view) {}

  default void decorateType(EngineContext ctx, TypeDecoration type) {}

  default void decorateFunction(EngineContext ctx, FunctionDecoration fn) {}
}
