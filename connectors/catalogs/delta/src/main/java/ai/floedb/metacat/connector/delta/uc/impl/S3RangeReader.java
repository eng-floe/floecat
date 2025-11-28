package ai.floedb.metacat.connector.delta.uc.impl;

import java.io.IOException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

final class S3RangeReader {

  private static final int DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024;

  private final S3Client s3;
  private final String bucket;
  private final String key;
  private final long fileLength;

  private final byte[] buffer;
  private long bufferStart = 0L;
  private int bufferLength = 0;

  private long totalGetRequests = 0;
  private long totalBytesFetched = 0;
  private long totalWindows = 0;
  private boolean closed = false;

  S3RangeReader(S3Client s3, String bucket, String key) throws IOException {
    this(s3, bucket, key, DEFAULT_CHUNK_SIZE);
  }

  S3RangeReader(S3Client s3, String bucket, String key, int chunkSize) throws IOException {
    this.s3 = s3;
    this.bucket = bucket;
    this.key = key;
    this.buffer = new byte[Math.max(1024, chunkSize)];

    try {
      HeadObjectResponse head =
          s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
      this.fileLength = head.contentLength();
    } catch (S3Exception e) {
      throw new IOException("Failed to get S3 object length for " + bucket + "/" + key, e);
    }
  }

  public synchronized void close() {
    if (!closed) {
      closed = true;
      /*
      System.out.println("[S3RangeReader] CLOSED for s3://" + bucket + "/" + key);
      System.out.println("  totalGetRequests = " + totalGetRequests);
      System.out.println("  totalWindows     = " + totalWindows);
      System.out.println("  totalBytesFetched = " + totalBytesFetched);
      System.out.println(
          "  avgWindowSize    = "
              + (totalWindows == 0 ? 0 : (totalBytesFetched / totalWindows))
              + " bytes");
      */
    }
  }

  long length() {
    return fileLength;
  }

  int readAt(long position, byte[] dest, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }

    if (position >= fileLength) {
      return -1;
    }

    ensureBufferFor(position);

    if (bufferLength <= 0) {
      return -1;
    }

    long bufferEnd = bufferStart + bufferLength;
    if (position < bufferStart || position >= bufferEnd) {
      ensureBufferFor(position);
      if (bufferLength <= 0) {
        return -1;
      }
      bufferEnd = bufferStart + bufferLength;
    }

    int available = (int) Math.min(bufferEnd - position, (long) len);
    int bufOff = (int) (position - bufferStart);
    System.arraycopy(buffer, bufOff, dest, off, available);
    return available;
  }

  private void ensureBufferFor(long position) throws IOException {
    if (position >= bufferStart && position < bufferStart + bufferLength) {
      return;
    }

    long start = position;
    long end = Math.min(start + buffer.length - 1L, fileLength - 1L);

    if (start > end) {
      bufferLength = 0;
      return;
    }

    String rangeHeader = "bytes=" + start + "-" + end;

    totalWindows++;
    totalGetRequests++;

    try (var obj =
        s3.getObject(
            GetObjectRequest.builder().bucket(bucket).key(key).range(rangeHeader).build())) {

      int off = 0;
      int n;
      while (off < buffer.length && (n = obj.read(buffer, off, buffer.length - off)) > 0) {
        off += n;
        totalBytesFetched += n;
      }

      bufferStart = start;
      bufferLength = off;
    } catch (S3Exception e) {
      if (e.statusCode() == 416) {
        bufferLength = 0;
        return;
      }
      throw new IOException(
          "S3 read error for " + bucket + "/" + key + " at range " + rangeHeader, e);
    }
  }
}
