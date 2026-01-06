package ai.floedb.floecat.kv.dynamodb;

import ai.floedb.floecat.kv.AbstractEntity;
import com.google.protobuf.MessageLite;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public abstract class AbstractEntityTest<M extends MessageLite> {

  protected abstract AbstractEntity<M> getEntity();

  @BeforeEach
  protected void resetTable() {
    getEntity().getKvStore().reset().await().indefinitely();
  }

  @AfterEach
  protected void dumpTable(TestInfo testInfo) {
    getEntity().getKvStore().dump("POST " + testInfo.getDisplayName()).await().indefinitely();
  }
}
