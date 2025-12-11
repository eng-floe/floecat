package ai.floedb.floecat.catalog.systemobjects.spi;

/**
 * SPI for engine plugins contributing system tables.
 *
 * <p>This is identical to SystemObjectProvider but conceptually indicates "plugin overrides".
 */
public interface EngineSystemTablesExtension extends SystemObjectProvider {}
