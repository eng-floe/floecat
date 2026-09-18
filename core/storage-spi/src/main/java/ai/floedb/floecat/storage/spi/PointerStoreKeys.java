/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.storage.spi;

/** Physical routing constants shared by pointer-store producers and consumers. */
public final class PointerStoreKeys {
  public static final String ACCOUNT_DELETION_FENCE_PREFIX = "/account-deletion-fences/";
  public static final String ACCOUNT_DELETION_FENCE_PARTITION_PREFIX = "_ACCOUNT_DELETION_FENCE/";
  public static final String ACCOUNT_DELETION_FENCE_SORT_KEY = "gate";

  public static final String ASSIGNMENT_FENCE_PREFIX = "/assignment-fence/";
  public static final String ASSIGNMENT_FENCE_PARTITION_PREFIX = "_ASSIGNMENT_FENCE/";
  public static final String ASSIGNMENT_FENCE_SORT_KEY = "fence";

  public static final String MEMBER_ASSIGNMENT_PREFIX = "/assignments/";
  public static final String MEMBER_ASSIGNMENT_PARTITION_PREFIX = "_MEMBER_ASSIGNMENT/";
  public static final String MEMBER_ASSIGNMENT_SORT_KEY = "index";

  private PointerStoreKeys() {}
}
