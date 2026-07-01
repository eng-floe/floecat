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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.Test;

class ColumnNdvSketchMergeTest {

  @Test
  void mergeTheta_twoSketches_unionsAndFinalizesEstimate() {
    ColumnNdv ndv = new ColumnNdv();
    ndv.mergeTheta(thetaBytes("a", "b"));
    ndv.mergeTheta(thetaBytes("c", "d"));

    ndv.finalizeTheta();

    assertNotNull(ndv.approx);
    assertEquals(4.0, ndv.approx.estimate, 0.01);
    assertEquals(1, ndv.sketches.size());
    assertEquals("apache-datasketches-theta-v1", ndv.sketches.get(0).type);
    assertFalse(ndv.sketches.get(0).data.length == 0);
    assertEquals(
        4.0, Sketches.wrapSketch(Memory.wrap(ndv.sketches.get(0).data)).getEstimate(), 0.01);
  }

  @Test
  void mergeApproxMetadata_preservesRowsThroughThetaFinalize() {
    ColumnNdv ndv = new ColumnNdv();
    ndv.mergeTheta(thetaBytes("a", "b"));
    ColumnNdv first = new ColumnNdv();
    first.approx = new NdvApprox();
    first.approx.rowsSeen = 10L;
    first.approx.rowsTotal = 100L;
    ColumnNdv second = new ColumnNdv();
    second.approx = new NdvApprox();
    second.approx.rowsSeen = 20L;
    second.approx.rowsTotal = 200L;

    ndv.mergeApproxMetadata(first);
    ndv.mergeApproxMetadata(second);
    ndv.finalizeTheta();

    assertNotNull(ndv.approx);
    assertEquals(30L, ndv.approx.rowsSeen);
    assertEquals(300L, ndv.approx.rowsTotal);
    assertEquals("apache-datasketches-theta", ndv.approx.method);
  }

  @Test
  void finalizeTheta_withoutThetaLeavesExistingSketchesUnchanged() {
    ColumnNdv ndv = new ColumnNdv();
    ndv.sketches.add(opaqueSketch("apache-datasketches-hll-v1"));

    ndv.finalizeTheta();

    assertEquals(1, ndv.sketches.size());
    assertEquals("apache-datasketches-hll-v1", ndv.sketches.get(0).type);
  }

  @Test
  void finalizeTheta_thetaReplacesExistingSketchPayloads() {
    ColumnNdv ndv = new ColumnNdv();
    ndv.mergeTheta(thetaBytes("a"));
    ndv.sketches.add(opaqueSketch("external-tuple-v1"));

    ndv.finalizeTheta();

    assertEquals(1, ndv.sketches.size());
    assertEquals("apache-datasketches-theta-v1", ndv.sketches.get(0).type);
  }

  @Test
  void staticOnceProvider_mergesThetaAndIgnoresNonThetaSketches() {
    ColumnNdv source = new ColumnNdv();
    source.sketches.add(thetaSketch("a", "b"));
    source.sketches.add(opaqueSketch("apache-datasketches-hll-v1"));

    ColumnNdv sink = new ColumnNdv();
    new StaticOnceNdvProvider(Map.of("col", source))
        .contributeNdv("file.parquet", Map.of("col", sink));

    sink.finalizeTheta();

    assertEquals(2.0, sink.approx.estimate, 0.01);
    assertEquals(1, sink.sketches.size());
    assertEquals("apache-datasketches-theta-v1", sink.sketches.get(0).type);
  }

  private static byte[] thetaBytes(String... values) {
    UpdateSketch sketch = UpdateSketch.builder().build();
    for (String value : values) {
      sketch.update(value);
    }
    return sketch.compact().toByteArray();
  }

  private static NdvSketch thetaSketch(String... values) {
    NdvSketch sketch = new NdvSketch();
    sketch.type = "apache-datasketches-theta-v1";
    sketch.data = thetaBytes(values);
    return sketch;
  }

  private static NdvSketch opaqueSketch(String type) {
    NdvSketch sketch = new NdvSketch();
    sketch.type = type;
    sketch.data = new byte[] {1, 2, 3};
    return sketch;
  }
}
