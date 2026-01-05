package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.query.rpc.CatalogBundleChunk;
import ai.floedb.floecat.query.rpc.CatalogBundleEnd;
import ai.floedb.floecat.query.rpc.CatalogBundleHeader;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.service.query.impl.QueryContext;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

@ApplicationScoped
public class CatalogBundleService {

  public Multi<CatalogBundleChunk> stream(
      String correlationId, QueryContext ctx, List<TableReferenceCandidate> tables) {
    CatalogBundleHeader header = CatalogBundleHeader.newBuilder().build();

    CatalogBundleChunk headerChunk =
        CatalogBundleChunk.newBuilder()
            .setQueryId(ctx.getQueryId())
            .setSeq(1)
            .setHeader(header)
            .build();

    CatalogBundleEnd end =
        CatalogBundleEnd.newBuilder().setRelationCount(0).setCustomObjectCount(0).build();

    CatalogBundleChunk endChunk =
        CatalogBundleChunk.newBuilder().setQueryId(ctx.getQueryId()).setSeq(2).setEnd(end).build();

    return Multi.createFrom().items(headerChunk, endChunk);
  }
}
