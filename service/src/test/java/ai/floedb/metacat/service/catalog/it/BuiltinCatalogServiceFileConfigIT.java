package ai.floedb.metacat.service.catalog.it;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.rpc.BuiltinCatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetBuiltinCatalogRequest;
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

  private static final Metadata.Key<String> ENGINE_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("metacat")
  BuiltinCatalogServiceGrpc.BuiltinCatalogServiceBlockingStub builtins;

  /** Uses a temporary filesystem directory to ensure file:// locations work. */
  @Test
  void loadsCatalogFromFileSystem() {
    var stub =
        builtins.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata("filetest")));
    var resp = stub.getBuiltinCatalog(GetBuiltinCatalogRequest.getDefaultInstance());
    assertThat(resp.hasCatalog()).isTrue();
    assertThat(resp.getCatalog().getVersion()).isEqualTo("filetest");
    assertThat(resp.getCatalog().getFunctions(0).getName()).isEqualTo("pg_catalog.identity");
  }

  private static Metadata metadata(String engineVersion) {
    Metadata m = new Metadata();
    m.put(ENGINE_HEADER, engineVersion);
    return m;
  }

  /** Quarkus profile that points the builtin loader at a temp directory. */
  public static class FileProfile implements QuarkusTestProfile {
    private static final Path DIR;
    private static final Path FILE;

    public FileProfile() {}

    static {
      try {
        DIR = Files.createTempDirectory("builtins-file");
        FILE = DIR.resolve("builtin_catalog_filetest.pbtxt");
        Files.writeString(
            FILE,
            """
            version: "filetest"
            functions { name: "pg_catalog.identity" argument_types: "pg_catalog.int4" return_type: "pg_catalog.int4" }
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
