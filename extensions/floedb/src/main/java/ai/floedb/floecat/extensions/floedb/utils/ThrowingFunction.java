package ai.floedb.floecat.extensions.floedb.utils;

@FunctionalInterface
public interface ThrowingFunction<I, O> {
  O apply(I in) throws Exception;
}
