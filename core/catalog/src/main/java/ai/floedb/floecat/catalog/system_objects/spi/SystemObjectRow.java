package ai.floedb.floecat.catalog.system_objects.spi;

/**
 * A single system object row.
 *
 * <p>Wrapper around Object[] with NO defensive copying. Caller must guarantee correct width for the
 * schema.
 */
public record SystemObjectRow(Object[] values) {}
