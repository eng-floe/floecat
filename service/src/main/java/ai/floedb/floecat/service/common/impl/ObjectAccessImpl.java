package ai.floedb.floecat.service.common.impl;

import ai.floedb.floecat.common.rpc.ObjectAccess;
import ai.floedb.floecat.common.rpc.PresignFilesRequest;
import ai.floedb.floecat.common.rpc.PresignFilesResponse;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import java.util.ArrayList;
import org.jboss.logging.Logger;

/**
 * Minimal presign stub: echoes file paths as "presigned URLs". Replace with real presigning when
 * storage access is wired.
 */
@GrpcService
public class ObjectAccessImpl extends BaseServiceImpl implements ObjectAccess {

  private static final Logger LOG = Logger.getLogger(ObjectAccess.class);

  @Override
  public Uni<PresignFilesResponse> presignFiles(PresignFilesRequest request) {
    var L = LogHelper.start(LOG, "PresignFiles");
    return mapFailures(
            run(
                () -> {
                  var urls = new ArrayList<String>();
                  for (var p : request.getFilePathList()) {
                    urls.add(p);
                  }
                  return PresignFilesResponse.newBuilder().addAllPresignedUrl(urls).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
