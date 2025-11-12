package ai.floedb.metacat.connector.delta.uc.impl;

import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

final class S3V2FileSystemClient implements FileIO {
  private final S3Client s3;

  S3V2FileSystemClient(S3Client s3) {
    this.s3 = s3;
  }

  @Override
  public InputFile newInputFile(String path, long fileSize) {
    return new S3InputFile(s3, resolvePath(path), fileSize);
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
    var bucket = u.getHost();
    var key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
    try {
      var head = s3.headObject(b -> b.bucket(bucket).key(key));
      return FileStatus.of(path, head.contentLength(), Instant.now().toEpochMilli());
    } catch (S3Exception e) {
      if (e.statusCode() == 404) throw new IOException("File not found: " + path, e);
      throw new IOException("Failed to get file status for: " + path, e);
    }
  }

  @Override
  public CloseableIterator<FileStatus> listFrom(String filePath) throws IOException {
    final String resolved = resolvePath(filePath);
    final URI u = URI.create(resolved);
    final String bucket = u.getHost();
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
        HeadObjectResponse head = s3.headObject(b -> b.bucket(bucket).key(startKey));
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
      private java.util.Iterator<S3Object> pageIter = null;
      private boolean yieldedFirst = (firstStatus == null);
      private FileStatus firstToYield = firstStatus;
      private boolean closed = false;

      private void fetchNextPage() {
        ListObjectsV2Request.Builder req =
            ListObjectsV2Request.builder().bucket(bucket).prefix(dirPrefix).maxKeys(1000);

        req = req.startAfter(startKey);

        if (continuationToken != null) {
          req = req.continuationToken(continuationToken);
        }

        ListObjectsV2Response resp = s3.listObjectsV2(req.build());
        continuationToken = resp.isTruncated() ? resp.nextContinuationToken() : null;

        List<S3Object> objs = resp.contents();
        pageIter =
            (objs == null)
                ? java.util.Collections.<S3Object>emptyList().iterator()
                : objs.iterator();
      }

      @Override
      public boolean hasNext() {
        if (closed) {
          return false;
        }

        if (!yieldedFirst && firstToYield != null) {
          return true;
        }

        while (true) {
          if (pageIter != null && pageIter.hasNext()) return true;
          if (continuationToken == null && pageIter != null) return false;
          fetchNextPage();
        }
      }

      @Override
      public FileStatus next() {
        if (closed) throw new java.util.NoSuchElementException("Iterator closed");
        if (!yieldedFirst && firstToYield != null) {
          yieldedFirst = true;
          FileStatus out = firstToYield;
          firstToYield = null;
          return out;
        }
        while (true) {
          if (pageIter != null && pageIter.hasNext()) {
            S3Object o = pageIter.next();
            String key = o.key();

            if (key == null || key.endsWith("/")) {
              continue;
            }

            if (!key.startsWith(dirPrefix)) {
              continue;
            }

            String fullPath = "s3://" + bucket + "/" + key;
            long size = (o.size() != null) ? o.size() : 0L;
            long mod =
                (o.lastModified() != null)
                    ? o.lastModified().toEpochMilli()
                    : Instant.now().toEpochMilli();
            return FileStatus.of(fullPath, size, mod);
          }
          if (continuationToken == null) {
            throw new NoSuchElementException();
          }
          fetchNextPage();
        }
      }

      @Override
      public void close() {
        closed = true;
        pageIter = null;
      }
    };
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    var u = URI.create(resolvePath(path));
    if (u.getHost() == null) {
      throw new IOException("Invalid S3 path for mkdirs: " + path);
    }
    return true;
  }

  @Override
  public Optional<String> getConf(String confKey) {
    return Optional.empty();
  }

  static final class S3InputFile implements InputFile {
    private final S3Client s3;
    private final String resolvedPath;
    private long fileSize;

    S3InputFile(S3Client s3, String resolvedPath, long fileSize) {
      this.s3 = s3;
      this.resolvedPath = resolvedPath;
      this.fileSize = fileSize;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      var u = URI.create(resolvedPath);
      var bucket = u.getHost();
      var key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
      return new S3SeekableInputStream(s3, bucket, key);
    }

    @Override
    public long length() throws IOException {
      if (fileSize == -1) {
        var u = URI.create(resolvedPath);
        var bucket = u.getHost();
        var key = u.getPath().startsWith("/") ? u.getPath().substring(1) : u.getPath();
        try {
          var head = s3.headObject(b -> b.bucket(bucket).key(key));
          fileSize = head.contentLength();
        } catch (S3Exception e) {
          throw new IOException("Failed to get file length: " + resolvedPath, e);
        }
      }
      return fileSize;
    }

    @Override
    public String path() {
      return resolvedPath;
    }
  }

  static final class S3SeekableInputStream extends SeekableInputStream {
    private final S3Client s3;
    private final String bucket;
    private final String key;
    private long pos = 0;
    private boolean closed = false;

    S3SeekableInputStream(S3Client s3, String bucket, String key) {
      this.s3 = s3;
      this.bucket = bucket;
      this.key = key;
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      int read = read(b, 0, 1);
      if (read == -1) {
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

      long start = pos;
      long end = start + len - 1;
      try (var obj =
          s3.getObject(
              GetObjectRequest.builder()
                  .bucket(bucket)
                  .key(key)
                  .range("bytes=" + start + "-" + end)
                  .build())) {
        int r = obj.read(b, off, len);
        if (r > 0) {
          pos += r;
        }

        return r;
      } catch (S3Exception e) {
        if (e.statusCode() == 416) {
          return -1;
        }

        throw new IOException("Error reading from S3 at position " + start, e);
      }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int currentRead = read(b, off + bytesRead, len - bytesRead);
        if (currentRead == -1) {
          throw new IOException("Reached end of stream before reading fully required bytes.");
        }
        bytesRead += currentRead;
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
    }
  }
}
