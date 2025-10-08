package ai.floedb.metacat.service.repo.util;

@FunctionalInterface
public interface ProtoParser<T> {
  T parse(byte[] bytes) throws Exception;
}