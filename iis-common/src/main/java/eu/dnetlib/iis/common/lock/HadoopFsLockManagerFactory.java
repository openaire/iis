package eu.dnetlib.iis.common.lock;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Hadoop FileSystem based lock manager factory.
 * <p>
 * Implements distributed locking by creating a sentinel lock file directly on
 * the target storage using Hadoop's {@link FileSystem} API. Works with any
 * Hadoop-supported storage backend — HDFS, S3 (via S3A), Ceph (RadosGW S3A),
 * etc. — without requiring an external coordination service such as ZooKeeper.
 * <p>
 * Locking behaviour:
 * <ul>
 *   <li>On <strong>HDFS</strong> the underlying {@code create(path, overwrite=false)}
 *       call is performed atomically by the NameNode, providing strong mutual
 *       exclusion guarantees.</li>
 *   <li>On <strong>S3-compatible stores</strong> the atomicity depends on
 *       server-side conditional-write support (S3 conditional writes,
 *       Ceph RadosGW object versioning, etc.). For typical low-concurrency
 *       cache-write scenarios the combination of optimistic create and
 *       TTL-based stale lock eviction provides sufficient protection.</li>
 * </ul>
 * <p>
 * The lock file is placed at {@code <dir>/.iis_lock} and contains the
 * lock-acquisition timestamp (ms since epoch) so that stale locks left behind
 * by crashed processes can be detected and evicted automatically.
 * <p>
 * Configuration properties (read from the Hadoop {@link Configuration} passed
 * to {@link #instantiate}):
 * <table border="1">
 *   <tr><th>Property</th><th>Default</th><th>Description</th></tr>
 *   <tr><td>{@value #PARAM_LOCK_TTL_MS}</td><td>1 800 000 (30 min)</td>
 *       <td>Milliseconds after which an un-released lock is considered stale
 *           and eligible for forced removal.</td></tr>
 *   <tr><td>{@value #PARAM_LOCK_RETRY_DELAY_MS}</td><td>5 000 (5 s)</td>
 *       <td>Milliseconds to wait between successive acquisition attempts.</td></tr>
 *   <tr><td>{@value #PARAM_LOCK_MAX_RETRIES}</td><td>360</td>
 *       <td>Maximum number of acquisition attempts before throwing
 *           {@link RuntimeException}.</td></tr>
 * </table>
 *
 * @author mhorst
 */
public class HadoopFsLockManagerFactory implements LockManagerFactory {

    /** Name of the sentinel file written inside the locked directory. */
    public static final String LOCK_FILE_NAME = ".iis_lock";

    /** Hadoop configuration key for the lock TTL in milliseconds. */
    public static final String PARAM_LOCK_TTL_MS = "iis.lock.ttl.ms";

    /** Hadoop configuration key for the retry delay in milliseconds. */
    public static final String PARAM_LOCK_RETRY_DELAY_MS = "iis.lock.retry.delay.ms";

    /** Hadoop configuration key for the maximum number of acquisition retries. */
    public static final String PARAM_LOCK_MAX_RETRIES = "iis.lock.max.retries";

    /** Default lock TTL: 30 minutes. */
    public static final long DEFAULT_LOCK_TTL_MS = 30L * 60_000L;

    /** Default delay between retry attempts: 5 seconds. */
    public static final long DEFAULT_LOCK_RETRY_DELAY_MS = 5_000L;

    /** Default maximum retries: 360 × 5 s ≈ 30 minutes total wait. */
    public static final int DEFAULT_LOCK_MAX_RETRIES = 360;

    private static final Logger log = Logger.getLogger(HadoopFsLockManagerFactory.class);

    // -------------------------------------------------------------------------

    @Override
    public LockManager instantiate(Configuration hadoopConf) {
        long lockTtlMs    = hadoopConf.getLong(PARAM_LOCK_TTL_MS,         DEFAULT_LOCK_TTL_MS);
        long retryDelayMs = hadoopConf.getLong(PARAM_LOCK_RETRY_DELAY_MS, DEFAULT_LOCK_RETRY_DELAY_MS);
        int  maxRetries   = hadoopConf.getInt (PARAM_LOCK_MAX_RETRIES,    DEFAULT_LOCK_MAX_RETRIES);

        // Per-path fair in-JVM mutex table.
        // Guarantees that, within the same JVM, only one thread at a time competes
        // for the filesystem lock, making LocalFileSystem behave atomically in tests
        // and preventing redundant cross-process racing in production.
        ConcurrentHashMap<String, ReentrantLock> jvmLocks = new ConcurrentHashMap<>();

        return new LockManager() {

            @Override
            public void obtain(String dir) throws Exception {
                Path lockFile = lockPath(dir);
                FileSystem fs = FileSystem.get(lockFile.toUri(), hadoopConf);

                // Acquire in-JVM mutex; released by release() or on failure below.
                ReentrantLock jvmLock = jvmLocks.computeIfAbsent(dir, k -> new ReentrantLock(true));
                jvmLock.lock();
                boolean fsLockAcquired = false;
                try {
                    // Ensure the parent directory exists (first-ever run on this cache root).
                    fs.mkdirs(lockFile.getParent());
                    log.info("Trying to obtain lock: " + lockFile);

                    for (int attempt = 0; attempt <= maxRetries; attempt++) {
                        evictStaleLockIfPresent(fs, lockFile, lockTtlMs);
                        try {
                            createLockFile(fs, lockFile);
                            log.info("Lock obtained: " + lockFile);
                            fsLockAcquired = true;
                            return; // jvmLock stays held — released by release()
                        } catch (FileAlreadyExistsException e) {
                            if (attempt < maxRetries) {
                                log.info("Lock is held by another process (" + lockFile + "), retrying in "
                                        + retryDelayMs + " ms (attempt " + (attempt + 1) + "/" + maxRetries + ")");
                                Thread.sleep(retryDelayMs);
                            }
                        }
                    }
                    throw new RuntimeException(
                            "Failed to acquire lock at " + lockFile + " after " + maxRetries + " attempts");
                } finally {
                    // Release the JVM mutex if we did not succeed in acquiring the FS lock.
                    if (!fsLockAcquired) {
                        jvmLock.unlock();
                    }
                }
            }

            @Override
            public void release(String dir) throws Exception {
                Path lockFile = lockPath(dir);
                FileSystem fs = FileSystem.get(lockFile.toUri(), hadoopConf);
                log.info("Releasing lock: " + lockFile);
                try {
                    if (!fs.delete(lockFile, false)) {
                        log.warn("Lock file not found during release — it may have been evicted as stale: " + lockFile);
                    }
                } finally {
                    ReentrantLock jvmLock = jvmLocks.get(dir);
                    if (jvmLock != null && jvmLock.isHeldByCurrentThread()) {
                        jvmLock.unlock();
                    }
                }
            }
        };
    }

    // --------------------------------- PACKAGE-PRIVATE -----------------------

    /** Resolves the lock file path for a given directory path. */
    static Path lockPath(String dir) {
        String normalised = dir.endsWith("/") ? dir.substring(0, dir.length() - 1) : dir;
        return new Path(normalised + "/" + LOCK_FILE_NAME);
    }

    // --------------------------------- PRIVATE -------------------------------

    /**
     * Atomically creates the lock file and writes the current timestamp into it.
     * Throws {@link FileAlreadyExistsException} when the file already exists.
     */
    private static void createLockFile(FileSystem fs, Path lockFile) throws Exception {
        // overwrite=false — atomic on HDFS, best-effort on object stores
        try (FSDataOutputStream out = fs.create(lockFile, false)) {
            out.write(Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * If the lock file exists and its recorded timestamp is older than
     * {@code lockTtlMs}, deletes it so a fresh acquisition attempt can proceed.
     * Errors are logged as warnings and swallowed to keep the retry loop alive.
     */
    private static void evictStaleLockIfPresent(FileSystem fs, Path lockFile, long lockTtlMs) {
        try {
            if (fs.exists(lockFile)) {
                long lockTimestamp = readLockTimestamp(fs, lockFile);
                long ageMs = System.currentTimeMillis() - lockTimestamp;
                if (ageMs > lockTtlMs) {
                    log.warn("Stale lock detected (age " + ageMs + " ms, TTL " + lockTtlMs
                            + " ms) — evicting: " + lockFile);
                    fs.delete(lockFile, false);
                }
            }
        } catch (Exception e) {
            log.warn("Could not check/evict stale lock at " + lockFile + ": " + e.getMessage());
        }
    }

    /** Reads the timestamp (ms since epoch) written by {@link #createLockFile}. */
    private static long readLockTimestamp(FileSystem fs, Path lockFile) throws Exception {
        try (InputStream is = fs.open(lockFile)) {
            return Long.parseLong(IOUtils.toString(is, StandardCharsets.UTF_8).trim());
        }
    }
}
