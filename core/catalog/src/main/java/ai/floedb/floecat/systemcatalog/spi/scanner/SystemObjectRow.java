package ai.floedb.floecat.systemcatalog.spi.scanner;

/**
 * A single system object row.
 *
 * <p>Wrapper around Object[] with NO defensive copying. Caller must guarantee correct width for the
 * schema.
 */
public record SystemObjectRow(Object[] values) {}
