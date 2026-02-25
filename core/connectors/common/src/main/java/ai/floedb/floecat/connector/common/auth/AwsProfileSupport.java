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

package ai.floedb.floecat.connector.common.auth;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;

public final class AwsProfileSupport {
  private AwsProfileSupport() {}

  public static Optional<AwsCredentialsProvider> resolveProfileProvider(
      Map<String, String> authProps) {
    if (authProps == null) {
      return Optional.empty();
    }
    String profile = authProps.get("aws.profile");
    if (profile == null || profile.isBlank()) {
      return Optional.empty();
    }

    ProfileCredentialsProvider.Builder builder =
        ProfileCredentialsProvider.builder().profileName(profile);

    String profilePath = authProps.get("aws.profile_path");
    if (profilePath != null && !profilePath.isBlank()) {
      ProfileFile.Type type = profileTypeForPath(profilePath);
      ProfileFile file = ProfileFile.builder().type(type).content(Path.of(profilePath)).build();
      builder.profileFile(file);
    }

    return Optional.of(builder.build());
  }

  public static void applyProfileProperties(
      Map<String, String> target, Map<String, String> authProps) {
    if (target == null || authProps == null) {
      return;
    }
    String awsProfile = authProps.get("aws.profile");
    if (awsProfile != null && !awsProfile.isBlank()) {
      target.put("aws.profile", awsProfile);
    }
    String awsProfilePath = authProps.get("aws.profile_path");
    if (awsProfilePath != null && !awsProfilePath.isBlank()) {
      target.put("aws.profile_path", awsProfilePath);
    }
  }

  private static ProfileFile.Type profileTypeForPath(String path) {
    String lower = path.toLowerCase(Locale.ROOT);
    if (lower.endsWith("credentials")) {
      return ProfileFile.Type.CREDENTIALS;
    }
    return ProfileFile.Type.CONFIGURATION;
  }
}
