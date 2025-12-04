package ai.floedb.metacat.service.catalog.it;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.query.rpc.BuiltinCatalogServiceGrpc;
import ai.floedb.metacat.query.rpc.GetBuiltinCatalogRequest;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(BuiltinCatalogServiceFileConfigIT.FileProfile.class)
class BuiltinCatalogServiceFileConfigIT {

  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("metacat")
  BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub builtins;

  @Test
  void loadsCatalogFromFileSystem() {
    var stub =
        builtins.withInterceptors(
            MetadataUtils.newAttachHeadersInterceptor(metadata("postgres", "16.0")));

    var resp = stub.getBuiltinCatalog(GetBuiltinCatalogRequest.getDefaultInstance());

    assertThat(resp.hasRegistry()).isTrue();

    // Validate full NameRef path / name
    NameRef f = resp.getRegistry().getFunctions(0).getName();
    assertThat(f.getPathList()).containsExactly("pg_catalog");
    assertThat(f.getName()).isEqualTo("identity");
  }

  private static Metadata metadata(String engineKind, String engineVersion) {
    Metadata m = new Metadata();
    m.put(ENGINE_KIND_HEADER, engineKind);
    m.put(ENGINE_VERSION_HEADER, engineVersion);
    return m;
  }

  public static class FileProfile implements QuarkusTestProfile {

    private static final Path DIR;
    private static final Path FILE;

    static {
      try {
        DIR = Files.createTempDirectory("builtins-file");
        FILE = DIR.resolve("postgres.pbtxt");

        Files.writeString(
            FILE,
            """
            functions {
              name { name: "identity" path: "pg_catalog" }
              argument_types { name: "int4" path: "pg_catalog" }
              return_type { name: "int4" path: "pg_catalog" }
            }
            """);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("metacat.builtins.location", "file:" + DIR.toAbsolutePath());
    }
  }
}
