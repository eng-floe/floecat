package ai.floedb.floecat.gateway.iceberg.rest.resources.support;

import ai.floedb.floecat.common.rpc.PageRequest;

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

  public static boolean hasPaging(PageRequest.Builder builder) {
    return builder != null;
  }
}
