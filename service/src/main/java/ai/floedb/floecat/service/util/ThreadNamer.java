package ai.floedb.floecat.service.util;

public class ThreadNamer implements AutoCloseable {
  private final String previousName;

  public ThreadNamer(String prefix) {
    Thread currentThread = Thread.currentThread();
    this.previousName = currentThread.getName();
    currentThread.setName(prefix + this.previousName);
  }

  @Override
  public void close() {
    Thread.currentThread().setName(previousName);
  }
}
