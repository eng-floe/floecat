package ai.floedb.floecat.extensions.floedb.utils;

public final class PayloadDescriptor<T> {

  private final String type;
  private final ThrowingFunction<byte[], T> decoder;

  private PayloadDescriptor(String type, ThrowingFunction<byte[], T> decoder) {
    this.type = type;
    this.decoder = decoder;
  }

  public String type() {
    return type;
  }

  public ThrowingFunction<byte[], T> decoder() {
    return decoder;
  }

  public static <T> PayloadDescriptor<T> of(String type, ThrowingFunction<byte[], T> decoder) {
    return new PayloadDescriptor<>(type, decoder);
  }
}
