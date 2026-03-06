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
package ai.floedb.floecat.service.query;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.metagraph.model.ViewNode;

/** Shared utilities for view name resolution used across query subsystems. */
public final class ViewContextUtils {

  private ViewContextUtils() {}

  /**
   * Enriches a base-relation {@link NameRef} with context from the view's creation environment so
   * that it re-resolves deterministically:
   *
   * <ul>
   *   <li>If {@code catalog} is blank, the query's default catalog name is substituted (when
   *       non-blank).
   *   <li>If {@code path} (schema) is empty, the view's {@code creationSearchPath} is used as the
   *       namespace path — the same schema that was in scope when the view was created.
   * </ul>
   *
   * <p>NameRefs that carry an explicit {@code resource_id} are returned unchanged; they already
   * identify the target directly and need no name-based enrichment.
   *
   * @param base the base-relation NameRef to enrich
   * @param view the view whose creation context supplies the search path
   * @param defaultCatalog the query's default catalog name, or blank if none
   */
  public static NameRef enrichForViewContext(NameRef base, ViewNode view, String defaultCatalog) {
    if (base.hasResourceId()) {
      return base;
    }
    boolean needsCatalog = base.getCatalog().isBlank() && !defaultCatalog.isBlank();
    boolean needsPath = base.getPathList().isEmpty() && !view.creationSearchPath().isEmpty();
    if (!needsCatalog && !needsPath) {
      return base;
    }
    NameRef.Builder b = base.toBuilder();
    if (needsCatalog) b.setCatalog(defaultCatalog);
    if (needsPath) b.addAllPath(view.creationSearchPath());
    return b.build();
  }
}
