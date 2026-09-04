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

package ai.floedb.floecat.catalog.access;

/** Which listing failures describe one branch of a catalog rather than the catalog. */
public final class CatalogTraversalFailures {

  private CatalogTraversalFailures() {}

  /**
   * Whether a failure listing one namespace's contents says nothing about the rest of the tree.
   *
   * <p>Shared so a walk that tolerates a branch and a walk that aborts on it cannot disagree. A
   * Unity workspace almost always exposes a system catalog whose schemas the integration principal
   * cannot enumerate; validation treated that as skippable while overlay reconciliation propagated
   * it, so an integration could validate and then fail to reconcile on the same catalog.
   *
   * <p>The four codes here are properties of the branch: it is not configured in a way this
   * provider can address, it is gone, the principal cannot see it, or the provider does not support
   * listing it. Everything else -- an unreachable upstream, a timeout, an internal fault -- will
   * answer the same way for every other branch, so tolerating it would walk the whole inventory to
   * collect one repeated failure.
   *
   * <p>Skippable is not the same as ignorable. A caller that tolerates a branch is deciding the
   * branch is optional; where the caller was told to look at that branch specifically, it should
   * surface the failure instead. That judgement belongs to the caller, not here.
   */
  public static boolean describesOneBranch(CatalogAccessException failure) {
    if (failure == null) {
      return false;
    }
    return switch (failure.code()) {
      case INVALID_CONFIGURATION, NOT_FOUND, PERMISSION_DENIED, UNSUPPORTED -> true;
      default -> false;
    };
  }
}
