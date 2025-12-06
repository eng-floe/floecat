package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StageCommitProcessorTest {
  private final StageCommitProcessor processor = new StageCommitProcessor();

  @BeforeEach
  void setUp() {
    // No-op: validateStageRequirements does not require initialized dependencies when
    // tableExists=true, but the setup hook makes future extensions easier.
  }

  @Test
  void assertCreateRequirementFailsWhenTableExists() {
    StageCommitException ex =
        assertThrows(
            StageCommitException.class,
            () ->
                processor.validateStageRequirements(
                    List.of(Map.of("type", "assert-create")),
                    "cat",
                    List.of("db"),
                    "orders",
                    true));

    assertEquals("assert-create failed", ex.getMessage());
  }
}
