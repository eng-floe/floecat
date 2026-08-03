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
package ai.floedb.floecat.service.concurrent;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.enterprise.util.Nonbinding;
import jakarta.interceptor.InterceptorBinding;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Marks a repository read whose store round-trip must run under the process-wide metadata-I/O
 * ceiling ({@code floecat.query.metadata_io.max_concurrency}). The {@link
 * MetadataIoAdmissionInterceptor} acquires a permit around the call automatically, so admission is
 * a property of the store method itself — no caller has to opt in, and none can bypass it.
 * Admission is re-entrant, so a read reached from within an already-admitted scope reuses the held
 * permit.
 *
 * <p>Placed on read methods only: writes travel the mutation/reconcile path and must not queue
 * behind the read ceiling. Apply at method level (not the whole repository) for exactly that
 * reason.
 */
@InterceptorBinding
@Inherited
@Target({METHOD, TYPE})
@Retention(RUNTIME)
public @interface BoundMetadataIo {
  /**
   * Reserved for future per-call tuning; non-binding so all annotated reads share one interceptor.
   */
  @Nonbinding
  boolean value() default true;
}
