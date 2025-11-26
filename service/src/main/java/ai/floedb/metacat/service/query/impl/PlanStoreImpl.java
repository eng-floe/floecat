package ai.floedb.metacat.service.query.impl;

import ai.floedb.metacat.query.rpc.GetPlanBundleRequest;
import ai.floedb.metacat.query.rpc.GetPlanBundleResponse;
import ai.floedb.metacat.query.rpc.PlanStore;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

/** Stub plan store: returns empty bundles. Replace with persisted snapshot->plan bundle lookup. */
@GrpcService
public class PlanStoreImpl extends BaseServiceImpl implements PlanStore {

  private static final Logger LOG = Logger.getLogger(PlanStore.class);

  @Override
  public Uni<GetPlanBundleResponse> getPlanBundle(GetPlanBundleRequest request) {
    var L = LogHelper.start(LOG, "GetPlanBundle");
    return mapFailures(run(() -> GetPlanBundleResponse.newBuilder().build()), correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
