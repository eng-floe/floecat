/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
