package eu.dnetlib.iis.common.lock;

import com.google.common.base.Stopwatch;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.lock.LockManagingProcess.LockMode;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link LockManagingProcess} test class.
 * @author mhorst
 *
 */
public class LockManagingProcessTest {

    
    private final LockManagingProcess lockManager = new LockManagingProcess();
    
    private final PortBindings portBindings = null;
    
    private final Configuration conf = new Configuration();
    
    private TestingServer zookeeperServer;

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @BeforeEach
    public void initialize() throws Exception {
        zookeeperServer = new TestingServer(true);
        conf.clear();
        conf.set(ZKFailoverController.ZK_QUORUM_KEY, "localhost:" + zookeeperServer.getPort());
    }
    
    @AfterEach
    public void shutdown() throws IOException {
        zookeeperServer.stop();
    }

    @Test
    public void testLockingWithoutNodeId() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());

        assertThrows(IllegalArgumentException.class, () -> lockManager.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testLockingWithoutMode() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");

        assertThrows(IllegalArgumentException.class, () -> lockManager.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testLockingWithoutQuorumKey() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
        conf.clear();

        assertThrows(IllegalArgumentException.class, () -> lockManager.run(portBindings, conf, parameters));
    }

    @Test
    public void testLockingWithInvalidQuorumKey() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
        conf.set(ZKFailoverController.ZK_QUORUM_KEY, "invalid");

        assertThrows(IllegalArgumentException.class, () -> lockManager.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testLockingWithUnsupportedMode() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, "unsupported");

        assertThrows(IllegalArgumentException.class, () -> lockManager.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testLockingAndUnlocking() {
        assertTimeout(TIMEOUT, () -> {
            Map<String, String> parameters = new HashMap<>();
            parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());

            lockManager.run(portBindings, conf, parameters);

            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.release.name());
            lockManager.run(portBindings, conf, parameters);

            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
            lockManager.run(portBindings, conf, parameters);
        });
    }
    
    @Test
    public void testLockingOnDifferentNodes() {
        assertTimeout(TIMEOUT, () -> {
            for (int i=0; i < 10; i++) {
                Map<String, String> parameters = new HashMap<>();
                parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
                parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid_" + i);
                lockManager.run(portBindings, conf, parameters);
            }
        });
    }

    @Test
    public void testUnlockingOnNotExistingNode() {
        assertTimeout(TIMEOUT, () -> {
            Map<String, String> parameters = new HashMap<>();
            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.release.name());
            parameters.put(LockManagingProcess.PARAM_NODE_ID, "not_existing_nodeid");
            lockManager.run(portBindings, conf, parameters);
        });
    }
    
    @Test
    public void testAsyncLocking() {
        assertTimeout(TIMEOUT, () -> {
            final String nodeId = "nodeid";
            final long timeout = 100;

            Map<String, String> parameters = new HashMap<>();
            parameters.put(LockManagingProcess.PARAM_NODE_ID, nodeId);
            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
            lockManager.run(portBindings, conf, parameters);

            final CompletableFuture<Long> threadWaitTimeFuture = new CompletableFuture<>();

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(() -> {
                try {
                    LockManagingProcess lockManager = new LockManagingProcess();
                    Map<String, String> params = new HashMap<>();
                    params.put(LockManagingProcess.PARAM_NODE_ID, nodeId);
                    params.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());

                    Stopwatch timer = new Stopwatch().start();

                    // obtaining lock
                    lockManager.run(portBindings, conf, params);

                    // returning processing time
                    threadWaitTimeFuture.complete(timer.elapsedMillis());

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(timeout);

            assertFalse(threadWaitTimeFuture.isDone());

            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.release.name());
            lockManager.run(portBindings, conf, parameters);

            assertTrue(threadWaitTimeFuture.get() >= timeout);
        });
    }
    
}
