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

package ai.floedb.floecat.connector.common.ndv;

import java.util.ArrayList;
import java.util.List;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;

public final class ColumnNdv {
  public NdvApprox approx;
  public List<NdvSketch> sketches = new ArrayList<>();

  public transient Union thetaUnion;

  public void mergeApproxMetadata(ColumnNdv other) {
    if (other == null || other.approx == null) {
      return;
    }
    if (approx == null) {
      approx = new NdvApprox();
    }
    if (other.approx.rowsSeen != null) {
      approx.rowsSeen = (approx.rowsSeen == null ? 0L : approx.rowsSeen) + other.approx.rowsSeen;
    }
    if (other.approx.rowsTotal != null) {
      approx.rowsTotal =
          (approx.rowsTotal == null ? 0L : approx.rowsTotal) + other.approx.rowsTotal;
    }
  }

  public void mergeTheta(byte[] serializedSketch) {
    if (serializedSketch == null || serializedSketch.length == 0) {
      return;
    }

    if (thetaUnion == null) {
      thetaUnion = SetOperation.builder().buildUnion();
    }

    Memory sketchMemory = Memory.wrap(serializedSketch);
    Sketch incomingSketch = Sketches.wrapSketch(sketchMemory);
    thetaUnion.union(incomingSketch);
  }

  public void mergeTheta(Sketch incomingSketch) {
    if (incomingSketch == null) {
      return;
    }

    if (thetaUnion == null) {
      thetaUnion = SetOperation.builder().buildUnion();
    }
    thetaUnion.union(incomingSketch);
  }

  /** Finalize the theta union. Idempotent when no transient theta union is present. */
  public void finalizeTheta() {
    if (thetaUnion == null) {
      return;
    }

    sketches.clear();

    var compact = thetaUnion.getResult(true, null);
    byte[] bytes = compact.toByteArray();
    NdvSketch sk = new NdvSketch();
    sk.type = "apache-datasketches-theta-v1";
    sk.data = bytes;
    sk.encoding = "raw";
    sk.compression = "none";
    sk.version = 1;
    sketches.add(sk);
    if (approx == null) approx = new NdvApprox();
    approx.estimate = compact.getEstimate();
    approx.method = "apache-datasketches-theta";
    thetaUnion = null;
  }
}
