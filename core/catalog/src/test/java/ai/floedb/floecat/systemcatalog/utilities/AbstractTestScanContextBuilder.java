package ai.floedb.floecat.systemcatalog.utilities;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;

abstract class AbstractTestScanContextBuilder {

  protected final ResourceId catalogId;
  protected final TestCatalogOverlay overlay = new TestCatalogOverlay();

  protected AbstractTestScanContextBuilder(ResourceId catalogId) {
    this.catalogId = catalogId;
  }

  public SystemObjectScanContext build() {
    return new SystemObjectScanContext(overlay, NameRef.getDefaultInstance(), catalogId);
  }
}
