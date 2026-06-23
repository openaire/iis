package eu.dnetlib.iis.common.lock;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link HadoopFsLockManagerFactory}.
 * Uses a local temporary directory as the filesystem so no cluster is required.
 *
 * @author mhorst
 */
class HadoopFsLockManagerFactoryTest {

    @TempDir
    File tempDir;

    private Configuration conf;
    private LockManager lockManager;
    private String lockDir;

    @BeforeEach
    void setUp() {
        conf = new Configuration();
        // Speed up tests: 100 ms retry delay, 10 retries, 5 s TTL
        conf.setLong(HadoopFsLockManagerFactory.PARAM_LOCK_RETRY_DELAY_MS, 100L);
        conf.setInt(HadoopFsLockManagerFactory.PARAM_LOCK_MAX_RETRIES, 10);
        conf.setLong(HadoopFsLockManagerFactory.PARAM_LOCK_TTL_MS, 5_000L);

        lockDir = tempDir.toURI().toString();
        lockManager = new HadoopFsLockManagerFactory().instantiate(conf);
    }

    // -------------------------------------------------------------------------
    // lockPath helper
    // -------------------------------------------------------------------------

    @Test
    void lockPath_shouldAppendLockFileName() {
        Path result = HadoopFsLockManagerFactory.lockPath("hdfs://nn1/tmp/cache");
        assertEquals("hdfs://nn1/tmp/cache/" + HadoopFsLockManagerFactory.LOCK_FILE_NAME, result.toString());
    }

    @Test
    void lockPath_shouldStripTrailingSlashBeforeAppending() {
        Path result = HadoopFsLockManagerFactory.lockPath("hdfs://nn1/tmp/cache/");
        assertEquals("hdfs://nn1/tmp/cache/" + HadoopFsLockManagerFactory.LOCK_FILE_NAME, result.toString());
    }

    // -------------------------------------------------------------------------
    // Basic obtain / release
    // -------------------------------------------------------------------------

    @Test
    void obtainAndRelease_shouldSucceed() throws Exception {
        lockManager.obtain(lockDir);
        assertLockFileExists(true);

        lockManager.release(lockDir);
        assertLockFileExists(false);
    }

    @Test
    void obtainTwiceSequentially_shouldSucceedAfterRelease() throws Exception {
        lockManager.obtain(lockDir);
        lockManager.release(lockDir);

        lockManager.obtain(lockDir);
        assertLockFileExists(true);
        lockManager.release(lockDir);
    }

    @Test
    void release_withNoLockFile_shouldNotThrow() {
        assertDoesNotThrow(() -> lockManager.release(lockDir));
    }

    @Test
    void lockFileContainsTimestamp() throws Exception {
        lockManager.obtain(lockDir);

        File lockFile = resolveLockFile();
        String content = FileUtils.readFileToString(lockFile, StandardCharsets.UTF_8).trim();
        long timestamp = Long.parseLong(content);
        assertTrue(timestamp > 0, "lock file should contain a positive timestamp");
        assertTrue(timestamp <= System.currentTimeMillis(), "timestamp should not be in the future");

        lockManager.release(lockDir);
    }

    // -------------------------------------------------------------------------
    // Stale lock eviction
    // -------------------------------------------------------------------------

    @Test
    void obtain_shouldEvictStaleLockAndAcquire() throws Exception {
        // Write a stale lock file (timestamp in the past beyond the TTL)
        File lockFile = resolveLockFile();
        lockFile.getParentFile().mkdirs();
        long staleTimestamp = System.currentTimeMillis() - 10_000L; // 10 s ago, TTL is 5 s
        FileUtils.writeStringToFile(lockFile, Long.toString(staleTimestamp), StandardCharsets.UTF_8);

        assertTimeout(Duration.ofSeconds(5), () -> lockManager.obtain(lockDir));
        assertTrue(lockFile.exists(), "lock file should be recreated after stale eviction");
        lockManager.release(lockDir);
    }

    @Test
    void obtain_shouldNotEvictFreshLock() throws Exception {
        // Write a "fresh" lock file held by another process (timestamp = now)
        // With max 10 retries × 100 ms = 1 s, the attempt must time out
        File lockFile = resolveLockFile();
        lockFile.getParentFile().mkdirs();
        FileUtils.writeStringToFile(lockFile, Long.toString(System.currentTimeMillis()), StandardCharsets.UTF_8);

        RuntimeException ex = assertThrows(RuntimeException.class, () -> lockManager.obtain(lockDir));
        assertTrue(ex.getMessage().contains("Failed to acquire lock"), "exception should mention failure to acquire");
    }

    // -------------------------------------------------------------------------
    // Concurrency
    // -------------------------------------------------------------------------

    @Test
    void concurrentObtain_onlyOneSucceedsAtATime() throws Exception {
        // Increase retries so second thread can wait for the first to release
        conf.setInt(HadoopFsLockManagerFactory.PARAM_LOCK_MAX_RETRIES, 100);
        LockManager lm = new HadoopFsLockManagerFactory().instantiate(conf);

        AtomicBoolean concurrentAccessDetected = new AtomicBoolean(false);
        AtomicBoolean inCriticalSection = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Runnable criticalSection = () -> {
                try {
                    lm.obtain(lockDir);
                    if (inCriticalSection.getAndSet(true)) {
                        concurrentAccessDetected.set(true);
                    }
                    Thread.sleep(200);
                    inCriticalSection.set(false);
                    lm.release(lockDir);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };

            CompletableFuture<Void> f1 = CompletableFuture.runAsync(criticalSection, pool);
            CompletableFuture<Void> f2 = CompletableFuture.runAsync(criticalSection, pool);

            assertTimeout(Duration.ofSeconds(30), () -> CompletableFuture.allOf(f1, f2).get());
            assertFalse(concurrentAccessDetected.get(), "two threads must not be in the critical section simultaneously");
        } finally {
            pool.shutdown();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void assertLockFileExists(boolean expected) {
        assertEquals(expected, resolveLockFile().exists(),
                "lock file existence should be " + expected);
    }

    private File resolveLockFile() {
        // Strip trailing "file:" scheme noise from tempDir URI, then resolve lock name
        return new File(tempDir, HadoopFsLockManagerFactory.LOCK_FILE_NAME);
    }
}
