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

package ai.floedb.floecat.extensions.floedb.tools;

import ai.floedb.floecat.extensions.floedb.FloeCatalogExtension;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.List;

/**
 * Validates the builtin catalog using the SAME validator path as runtime.
 *
 * <p>Usage: --engine=floedb (default floedb)
 */
public final class BuiltinCatalogValidatorTool {

  public static void main(String[] args) {
    String engine = "floedb";
    for (String a : args) {
      if (a.startsWith("--engine=")) engine = a.substring("--engine=".length());
    }

    FloeCatalogExtension ext = extensionFor(engine);

    SystemCatalogData catalog = ext.loadSystemCatalog();
    List<ValidationIssue> issues = ext.validate(catalog);

    if (!issues.isEmpty()) {
      System.err.println("Builtin catalog validation FAILED for engine=" + engine);
      for (ValidationIssue issue : issues) {
        System.err.println(" - " + issue);
      }
      System.exit(2);
    }

    System.out.println("Builtin catalog validation OK for engine=" + engine);
  }

  private static FloeCatalogExtension extensionFor(String engineKind) {
    return switch (engineKind) {
      case "floedb" -> new FloeCatalogExtension.FloeDb();
      case "floe-demo" -> new FloeCatalogExtension.FloeDemo();
      default -> throw new IllegalArgumentException("Unknown engine kind: " + engineKind);
    };
  }
}
