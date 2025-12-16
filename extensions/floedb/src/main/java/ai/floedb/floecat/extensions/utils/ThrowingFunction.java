package ai.floedb.floecat.extensions.utils;

@FunctionalInterface
public interface ThrowingFunction<I, O> {
  O apply(I in) throws Exception;
}
