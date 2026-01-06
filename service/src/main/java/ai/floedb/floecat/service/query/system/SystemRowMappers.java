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

package ai.floedb.floecat.service.query.system;

import ai.floedb.floecat.system.rpc.SystemTableRow;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;

public final class SystemRowMappers {

  public static SystemTableRow toProto(SystemObjectRow row) {
    var b = SystemTableRow.newBuilder();
    for (Object v : row.values()) {
      b.addValues(v == null ? "" : v.toString());
    }
    return b.build();
  }

  private SystemRowMappers() {}
}
