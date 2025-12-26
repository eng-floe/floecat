package ai.floedb.floecat.systemcatalog.spi.types;

import ai.floedb.floecat.metagraph.model.TypeNode;
import java.util.Optional;

public interface TypeLookup {
  Optional<TypeNode> findByName(String namespace, String name);
}
