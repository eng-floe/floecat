package ai.floedb.metacat.client.trino;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class MetacatConfig {

  private String metacatUri;
  private boolean coordinatorFileCaching = false;
  private String s3AccessKey;
  private String s3SecretKey;
  private String s3SessionToken;
  private String s3Region;
  private String s3Endpoint;
  private String s3StsRoleArn;
  private String s3StsRegion;
  private String s3StsEndpoint;
  private String s3RoleSessionName;

  @NotNull
  public String getMetacatUri() {
    return metacatUri;
  }

  @Config("metacat.uri")
  public MetacatConfig setMetacatUri(String uri) {
    this.metacatUri = uri;
    return this;
  }

  @Config("metacat.coordinator-file-caching")
  public MetacatConfig setCoordinatorFileCaching(boolean enabled) {
    this.coordinatorFileCaching = enabled;
    return this;
  }

  public boolean isCoordinatorFileCaching() {
    return coordinatorFileCaching;
  }

  public String getS3AccessKey() {
    return s3AccessKey;
  }

  @Config("metacat.s3.access-key")
  public MetacatConfig setS3AccessKey(String s3AccessKey) {
    this.s3AccessKey = s3AccessKey;
    return this;
  }

  public String getS3SecretKey() {
    return s3SecretKey;
  }

  @Config("metacat.s3.secret-key")
  public MetacatConfig setS3SecretKey(String s3SecretKey) {
    this.s3SecretKey = s3SecretKey;
    return this;
  }

  public String getS3SessionToken() {
    return s3SessionToken;
  }

  @Config("metacat.s3.session-token")
  public MetacatConfig setS3SessionToken(String s3SessionToken) {
    this.s3SessionToken = s3SessionToken;
    return this;
  }

  public String getS3Region() {
    return s3Region;
  }

  @Config("metacat.s3.region")
  public MetacatConfig setS3Region(String s3Region) {
    this.s3Region = s3Region;
    return this;
  }

  public String getS3Endpoint() {
    return s3Endpoint;
  }

  @Config("metacat.s3.endpoint")
  public MetacatConfig setS3Endpoint(String s3Endpoint) {
    this.s3Endpoint = s3Endpoint;
    return this;
  }

  public String getS3StsRoleArn() {
    return s3StsRoleArn;
  }

  @Config("metacat.s3.sts.role-arn")
  public MetacatConfig setS3StsRoleArn(String s3StsRoleArn) {
    this.s3StsRoleArn = s3StsRoleArn;
    return this;
  }

  public String getS3StsRegion() {
    return s3StsRegion;
  }

  @Config("metacat.s3.sts.region")
  public MetacatConfig setS3StsRegion(String s3StsRegion) {
    this.s3StsRegion = s3StsRegion;
    return this;
  }

  public String getS3StsEndpoint() {
    return s3StsEndpoint;
  }

  @Config("metacat.s3.sts.endpoint")
  public MetacatConfig setS3StsEndpoint(String s3StsEndpoint) {
    this.s3StsEndpoint = s3StsEndpoint;
    return this;
  }

  public String getS3RoleSessionName() {
    return s3RoleSessionName;
  }

  @Config("metacat.s3.role-session-name")
  public MetacatConfig setS3RoleSessionName(String s3RoleSessionName) {
    this.s3RoleSessionName = s3RoleSessionName;
    return this;
  }
}
