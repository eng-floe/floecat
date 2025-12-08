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

  public void finalizeTheta() {
    if (thetaUnion == null) {
      return;
    }

    var compact = thetaUnion.getResult(true, null);
    byte[] bytes = compact.toByteArray();

    sketches.clear();
    NdvSketch sk = new NdvSketch();
    sk.type = "apache-datasketches-theta-v1";
    sk.data = bytes;
    sk.encoding = "raw";
    sk.compression = "none";
    sk.version = 1;
    sketches.add(sk);

    if (approx == null) {
      approx = new NdvApprox();
    }

    approx.estimate = compact.getEstimate();
    approx.method = "apache-datasketches-theta";
    thetaUnion = null;
  }
}
