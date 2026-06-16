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

package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.reconciler.rpc.ClearReconcileQueueRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import java.io.PrintStream;
import java.util.List;

final class ReconcilerCliSupport {

  private ReconcilerCliSupport() {}

  static void handle(
      List<String> args,
      PrintStream out,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl) {
    if (args.isEmpty()) {
      printUsage(out);
      return;
    }
    switch (args.get(0)) {
      case "queue" -> queue(args.subList(1, args.size()), out, reconcileControl);
      default -> out.println("unknown subcommand");
    }
  }

  private static void queue(
      List<String> args,
      PrintStream out,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl) {
    if (args.isEmpty()) {
      out.println("usage: reconciler queue clear --force");
      return;
    }
    switch (args.get(0)) {
      case "clear" -> clearQueue(args.subList(1, args.size()), out, reconcileControl);
      default -> out.println("unknown subcommand");
    }
  }

  private static void clearQueue(
      List<String> args,
      PrintStream out,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl) {
    if (!args.contains("--force")) {
      out.println("usage: reconciler queue clear --force");
      return;
    }
    var response =
        reconcileControl.clearReconcileQueue(
            ClearReconcileQueueRequest.newBuilder().setForce(true).build());
    out.println("jobs_cleared=" + response.getJobsCleared());
    out.println("pointers_deleted=" + response.getPointersDeleted());
    out.println("job_blob_prefixes_deleted=" + response.getJobBlobPrefixesDeleted());
  }

  static void printUsage(PrintStream out) {
    out.println("usage: reconciler queue clear --force");
  }
}
