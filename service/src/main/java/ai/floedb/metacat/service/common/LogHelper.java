package ai.floedb.metacat.service.common;

import org.jboss.logging.Logger;

public final class LogHelper {
  private final Logger log;
  private final String op;
  private final long startNs;

  private LogHelper(Logger log, String op) {
    this.log = log;
    this.op = op;
    this.startNs = System.nanoTime();
    log.infof("op=%s start", op);
  }

  public static LogHelper start(Logger log, String op) {
    return new LogHelper(log, op);
  }

  public void ok() {
    double ms = elapsedMs();
    log.infof("op=%s ok elapsedMs=%.1f", op, ms);
  }

  public void okf(String fmt, Object... args) {
    double ms = elapsedMs();
    log.infof("op=%s ok " + fmt + " elapsedMs=%.1f", merge(args, ms));
  }

  public void fail(Throwable t) {
    double ms = elapsedMs();
    log.errorf(t, "op=%s fail elapsedMs=%.1f", op, ms);
  }

  public void failf(String fmt, Object... args) {
    double ms = elapsedMs();
    log.errorf("op=%s fail " + fmt + " elapsedMs=%.1f", merge(args, ms));
  }

  public void warn(String msg) {
    double ms = elapsedMs();
    log.warnf("op=%s warn %s elapsedMs=%.1f", op, msg, ms);
  }

  private double elapsedMs() {
    return (System.nanoTime() - startNs) / 1e6;
  }

  private Object[] merge(Object[] args, double ms) {
    Object[] merged = new Object[args.length + 1];
    System.arraycopy(args, 0, merged, 0, args.length);
    merged[args.length] = ms;
    return merged;
  }
}
