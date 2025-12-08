package ai.floedb.floecat.catalog.system_objects.spi;

/**
 * SPI for engine plugins contributing system tables.
 *
 * <p>This is identical to SystemObjectProvider but conceptually indicates "plugin overrides".
 */
public interface EngineSystemTablesExtension extends SystemObjectProvider {}
