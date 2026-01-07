/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.client.trino;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class FloecatConfig {

  private String floecatUri;
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
  public String getFloecatUri() {
    return floecatUri;
  }

  @Config("floecat.uri")
  public FloecatConfig setFloecatUri(String uri) {
    this.floecatUri = uri;
    return this;
  }

  @Config("floecat.coordinator-file-caching")
  public FloecatConfig setCoordinatorFileCaching(boolean enabled) {
    this.coordinatorFileCaching = enabled;
    return this;
  }

  public boolean isCoordinatorFileCaching() {
    return coordinatorFileCaching;
  }

  public String getS3AccessKey() {
    return s3AccessKey;
  }

  @Config("floecat.s3.access-key")
  public FloecatConfig setS3AccessKey(String s3AccessKey) {
    this.s3AccessKey = s3AccessKey;
    return this;
  }

  public String getS3SecretKey() {
    return s3SecretKey;
  }

  @Config("floecat.s3.secret-key")
  public FloecatConfig setS3SecretKey(String s3SecretKey) {
    this.s3SecretKey = s3SecretKey;
    return this;
  }

  public String getS3SessionToken() {
    return s3SessionToken;
  }

  @Config("floecat.s3.session-token")
  public FloecatConfig setS3SessionToken(String s3SessionToken) {
    this.s3SessionToken = s3SessionToken;
    return this;
  }

  public String getS3Region() {
    return s3Region;
  }

  @Config("floecat.s3.region")
  public FloecatConfig setS3Region(String s3Region) {
    this.s3Region = s3Region;
    return this;
  }

  public String getS3Endpoint() {
    return s3Endpoint;
  }

  @Config("floecat.s3.endpoint")
  public FloecatConfig setS3Endpoint(String s3Endpoint) {
    this.s3Endpoint = s3Endpoint;
    return this;
  }

  public String getS3StsRoleArn() {
    return s3StsRoleArn;
  }

  @Config("floecat.s3.sts.role-arn")
  public FloecatConfig setS3StsRoleArn(String s3StsRoleArn) {
    this.s3StsRoleArn = s3StsRoleArn;
    return this;
  }

  public String getS3StsRegion() {
    return s3StsRegion;
  }

  @Config("floecat.s3.sts.region")
  public FloecatConfig setS3StsRegion(String s3StsRegion) {
    this.s3StsRegion = s3StsRegion;
    return this;
  }

  public String getS3StsEndpoint() {
    return s3StsEndpoint;
  }

  @Config("floecat.s3.sts.endpoint")
  public FloecatConfig setS3StsEndpoint(String s3StsEndpoint) {
    this.s3StsEndpoint = s3StsEndpoint;
    return this;
  }

  public String getS3RoleSessionName() {
    return s3RoleSessionName;
  }

  @Config("floecat.s3.role-session-name")
  public FloecatConfig setS3RoleSessionName(String s3RoleSessionName) {
    this.s3RoleSessionName = s3RoleSessionName;
    return this;
  }
}
