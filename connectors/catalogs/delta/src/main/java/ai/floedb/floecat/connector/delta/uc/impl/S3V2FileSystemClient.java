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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.aws.RefreshingAwsClient;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

final class S3V2FileSystemClient implements FileIO {
  private final RefreshingAwsClient<S3Client> s3;

  /**
   * Addresses the bucket named in each URI, always.
   *
   * <p>There is deliberately no override. Substituting a vended S3 access-point ARN for the host
   * was tried and removed: it retargets every URI, including the absolute paths in another bucket
   * that a Delta log may carry and a shallow clone always does, so a foreign path would be read
   * from the access point's bucket -- silently, when a key happens to exist there. A vend carrying
   * an access point has the ARN dropped at {@code SourceCatalogCredentialVendor} and is used
   * against the bucket, so nothing here is retargeted -- but do not read that as a guarantee that
   * no such credential arrives: one does, minus its ARN, and a grant that is genuinely
   * access-point-only fails at storage rather than here.
   */
  S3V2FileSystemClient(RefreshingAwsClient<S3Client> s3) {
    this.s3 = s3;
  }

  @Override
  public InputFile newInputFile(String path, long fileSize) {
    String resolved = resolvePath(path);
    if (isMissingCheckpoint(resolved)) {
      return new MissingInputFile(resolved);
    }
    return new S3InputFile(s3, resolved);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    throw new UnsupportedOperationException(
        "Writing files not implemented for read-only S3V2FileIO");
  }

  @Override
  public boolean delete(String path) {
    throw new UnsupportedOperationException(
        "Deleting files not implemented for read-only S3V2FileIO");
  }

  @Override
  public void copyFileAtomically(String sourcePath, String destinationPath, boolean overwrite) {
    throw new UnsupportedOperationException(
        "Copying files not implemented for read-only S3V2FileIO");
  }

  /**
   * An {@code s3://} path for a listed object, in a form the rest of this client can parse.
   *
   * <p>Every path here comes back through {@code resolvePath}, which calls {@code URI.create} --
   * and an S3 object key may hold characters that rejects, a space being the one that occurs. This
   * was built by concatenating the raw key, so a table under {@code db/my table} listed its Delta
   * log fine and then threw when the next entry was opened: the caller's own location could be
   * encoded, but a listing rebuilt every later path from the raw key and lost that.
   *
   * <p>Round-trips: {@code getPath} decodes, so the key asked of S3 is the key that was written.
   * For a key holding nothing {@code URI} objects to, the result is byte-identical to plain
   * concatenation, so only a path that would otherwise fail to parse is affected at all.
   */
  static String s3Uri(String bucket, String key) {
    // Segment by segment, so no leading slash is ever handed to URI as part of a "//" it would read
    // as an authority. An S3 key may begin with a slash, and for such a key the path is "//table",
    // whose first segment URI takes as a host and drops from getRawPath -- silently, so a fallback
    // never runs and S3 is asked for a key missing its first segment. Splitting keeps the empty
    // leading segment that slash represents, and no segment can hold a slash to re-parse.
    StringBuilder path = new StringBuilder();
    for (String segment : key.split("/", -1)) {
      try {
        String encoded = new URI(null, null, "/" + segment, null).getRawPath();
        path.append('/').append(encoded, 1, encoded.length());
      } catch (URISyntaxException notAUri) {
        path.append('/').append(segment);
      }
    }
    return "s3://" + bucket + path;
  }

  /**
   * The bucket a location addresses, falling back to the authority when it is not a hostname.
   *
   * <p>S3 permitted bucket names {@code java.net.URI} will not parse as a host -- an underscore is
   * the one that survives in older regions -- and for those {@code getHost} answers null while the
   * authority carries the name. {@code S3DeltaLogProbe.bucketOf} accepts such a name, so this has
   * to as well: a table the probe validates and the reader cannot address is one that reconciles
   * and then fails every scan. The fallback's shape matches that method -- an authority holding
   * userinfo or a port is not a bucket name.
   *
   * <p>Not shared with it because this module does not depend on catalog-access, and a connector
   * depending on it to parse a URI would be the wrong direction.
   */
  static String bucketOf(URI location) {
    String host = location.getHost();
    if (host != null && !host.isEmpty()) {
      return host;
    }
    String authority = location.getAuthority();
    if (authority == null
        || authority.isEmpty()
        || authority.indexOf('@') >= 0
        || authority.indexOf(':') >= 0) {
      return null;
    }
    return authority;
  }

  @Override
  public String resolvePath(String path) {
    if (path.startsWith("s3a://")) {
      return "s3://" + path.substring(6);
    }
    if (path.startsWith("s3://")) {
      return path;
    }
    throw new IllegalArgumentException("Unsupported file system path: " + path);
  }

  @Override
  public FileStatus getFileStatus(String path) throws IOException {
    var u = URI.create(resolvePath(path));
    var bucket = bucketOf(u);
    var key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
    try {
      var head = s3.call(client -> client.headObject(b -> b.bucket(bucket).key(key)));
      return FileStatus.of(path, head.contentLength(), Instant.now().toEpochMilli());
    } catch (S3Exception e) {
      if (e.statusCode() == 404) throw new IOException("File not found: " + path, e);
      throw new IOException("Failed to get file status for: " + path, e);
    }
  }

  private boolean isMissingCheckpoint(String resolvedPath) {
    if (!resolvedPath.endsWith("/_last_checkpoint")) {
      return false;
    }
    URI u = URI.create(resolvedPath);
    String bucket = bucketOf(u);
    String key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
    try {
      s3.callUnchecked(client -> client.headObject(b -> b.bucket(bucket).key(key)));
      return false;
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        return true;
      }
      throw e;
    }
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    final String resolved = resolvePath(filePath);
    final URI u = URI.create(resolved);
    final String bucket = bucketOf(u);
    if (bucket == null || bucket.isEmpty()) {
      throw new IOException("Invalid S3 path for listFrom: " + filePath);
    }
    final String fullKey = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();

    final int lastSlash = fullKey.lastIndexOf('/');
    final String dirPrefix;
    final String startKey;
    if (lastSlash < 0) {
      dirPrefix = "";
      startKey = fullKey;
    } else {
      dirPrefix = fullKey.substring(0, lastSlash + 1);
      startKey = fullKey;
    }

    final FileStatus firstStatus;
    {
      FileStatus fs = null;
      try {
        HeadObjectResponse head =
            s3.call(client -> client.headObject(b -> b.bucket(bucket).key(startKey)));
        fs =
            FileStatus.of(
                filePath,
                head.contentLength(),
                head.lastModified() != null
                    ? head.lastModified().toEpochMilli()
                    : Instant.now().toEpochMilli());
      } catch (S3Exception e) {
        fs = null;
      } catch (Exception e) {
        throw new IOException("Failed to probe start object for listFrom: " + filePath, e);
      }
      firstStatus = fs;
    }

    return new CloseableIterator<>() {
      private String continuationToken = null;
      private Iterator<S3Object> pageIter = null;
      private boolean yieldedFirst = (firstStatus == null);
      private FileStatus firstToYield = firstStatus;
      private FileStatus bufferedNext = null;
      private boolean closed = false;

      private void fetchNextPage() {
        ListObjectsV2Request.Builder req =
            ListObjectsV2Request.builder().bucket(bucket).prefix(dirPrefix).maxKeys(1000);

        req = req.startAfter(startKey);

        if (continuationToken != null) {
          req = req.continuationToken(continuationToken);
        }

        ListObjectsV2Request request = req.build();
        ListObjectsV2Response resp = s3.callUnchecked(client -> client.listObjectsV2(request));
        continuationToken = resp.isTruncated() ? resp.nextContinuationToken() : null;

        List<S3Object> objs = resp.contents();
        pageIter = (objs == null) ? Collections.<S3Object>emptyList().iterator() : objs.iterator();
      }

      @Override
      public boolean hasNext() {
        if (closed) {
          return false;
        }
        return ensureBufferedNext();
      }

      @Override
      public FileStatus next() {
        if (closed) {
          throw new NoSuchElementException("Iterator closed");
        }
        if (!ensureBufferedNext()) {
          throw new NoSuchElementException();
        }
        FileStatus out = bufferedNext;
        bufferedNext = null;
        return out;
      }

      @Override
      public void close() {
        closed = true;
        pageIter = null;
        bufferedNext = null;
      }

      private boolean ensureBufferedNext() {
        if (bufferedNext != null) {
          return true;
        }

        if (!yieldedFirst && firstToYield != null) {
          yieldedFirst = true;
          bufferedNext = firstToYield;
          firstToYield = null;
          return true;
        }

        while (true) {
          if (pageIter != null && pageIter.hasNext()) {
            S3Object o = pageIter.next();
            String key = o.key();
            if (key == null || key.endsWith("/") || !key.startsWith(dirPrefix)) {
              continue;
            }

            String fullPath = s3Uri(bucket, key);
            long size = (o.size() != null) ? o.size() : 0L;
            long mod =
                (o.lastModified() != null)
                    ? o.lastModified().toEpochMilli()
                    : Instant.now().toEpochMilli();
            bufferedNext = FileStatus.of(fullPath, size, mod);
            return true;
          }

          if (continuationToken == null && pageIter != null) {
            return false;
          }

          fetchNextPage();
        }
      }
    };
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    var u = URI.create(resolvePath(path));
    if (bucketOf(u) == null) {
      throw new IOException("Invalid S3 path for mkdirs: " + path);
    }
    return true;
  }

  @Override
  public Optional<String> getConf(String confKey) {
    return Optional.empty();
  }

  static final class S3InputFile implements InputFile {
    private final RefreshingAwsClient<S3Client> s3;
    private final String resolvedPath;

    private final String bucket;
    private final String key;
    private final S3RangeReader rangeReader;

    S3InputFile(RefreshingAwsClient<S3Client> s3, String resolvedPath) {
      this.s3 = s3;
      this.resolvedPath = resolvedPath;

      var u = URI.create(resolvedPath);
      this.bucket = bucketOf(u);
      this.key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();

      try {
        this.rangeReader = new S3RangeReader(s3, bucket, key);
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize S3RangeReader for " + resolvedPath, e);
      }
    }

    @Override
    public SeekableInputStream newStream() {
      return new S3SeekableInputStream(rangeReader);
    }

    @Override
    public long length() {
      return rangeReader.length();
    }

    @Override
    public String path() {
      return resolvedPath;
    }
  }

  static final class MissingInputFile implements InputFile {
    private final String resolvedPath;

    MissingInputFile(String resolvedPath) {
      this.resolvedPath = resolvedPath;
    }

    @Override
    public SeekableInputStream newStream() {
      return new MissingSeekableInputStream(resolvedPath);
    }

    @Override
    public long length() {
      return 0L;
    }

    @Override
    public String path() {
      return resolvedPath;
    }
  }

  static final class MissingSeekableInputStream extends SeekableInputStream {
    private final String path;

    MissingSeekableInputStream(String path) {
      this.path = path;
    }

    @Override
    public int read() throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public long getPos() throws IOException {
      return 0L;
    }

    @Override
    public void seek(long newPos) throws IOException {
      throw new FileNotFoundException("File not found: " + path);
    }

    @Override
    public void close() {}
  }

  static final class S3SeekableInputStream extends SeekableInputStream {
    private final S3RangeReader reader;
    private long pos = 0;
    private boolean closed = false;

    S3SeekableInputStream(S3RangeReader reader) {
      this.reader = reader;
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      int n = read(b, 0, 1);
      if (n == -1) {
        return -1;
      }

      return b[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (closed) {
        throw new IOException("stream closed");
      }
      if (len == 0) {
        return 0;
      }

      int n = reader.readAt(pos, b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      int done = 0;
      while (done < len) {
        int n = read(b, off + done, len - done);
        if (n == -1) {
          throw new IOException("Reached EOF while reading fully");
        }
        done += n;
      }
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0) {
        throw new IOException("negative seek");
      }

      this.pos = newPos;
    }

    @Override
    public void close() {
      closed = true;
      reader.close();
    }
  }
}
