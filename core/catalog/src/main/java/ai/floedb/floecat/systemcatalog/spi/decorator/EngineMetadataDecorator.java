package ai.floedb.floecat.systemcatalog.spi.decorator;

/**
 * Engine metadata decoration hook exposed by {@link
 * ai.floedb.floecat.service.query.catalog.CatalogBundleService}.
 *
 * <p>Implementations are invoked during bundle construction and receive normalized engine kind +
 * version. Decoration remains optional and is not part of scanner implementations.
 */
public interface EngineMetadataDecorator {

  default void decorateRelation(
      String engineKind, String engineVersion, RelationDecoration relation) {
    // no-op by default
  }

  default void decorateColumn(String engineKind, String engineVersion, ColumnDecoration column) {
    // no-op by default
  }
}
