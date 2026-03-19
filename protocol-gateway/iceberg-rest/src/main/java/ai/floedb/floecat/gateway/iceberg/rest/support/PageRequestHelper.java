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

package ai.floedb.floecat.gateway.iceberg.rest.support;

import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.PageResponse;

public final class PageRequestHelper {
  private PageRequestHelper() {}

  public static PageRequest.Builder builder(String pageToken, Integer pageSize) {
    if (pageToken == null && pageSize == null) {
      return null;
    }
    PageRequest.Builder builder = PageRequest.newBuilder();
    if (pageToken != null) {
      builder.setPageToken(pageToken);
    }
    if (pageSize != null) {
      builder.setPageSize(pageSize);
    }
    return builder;
  }

  public static String nextToken(PageResponse page) {
    if (page == null) {
      return null;
    }
    String token = page.getNextPageToken();
    return token == null || token.isBlank() ? null : token;
  }
}
