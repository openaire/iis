package eu.dnetlib.iis.common.lock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import com.google.common.base.Stopwatch;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.lock.LockManagingProcess.LockMode;

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
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(5);
    
    @Before
    public void initialize() throws Exception {
        zookeeperServer = new TestingServer(true);
        conf.clear();
        conf.set(ZKFailoverController.ZK_QUORUM_KEY, "localhost:" + zookeeperServer.getPort());
    }
    
    @After
    public void shutdown() throws IOException {
        zookeeperServer.stop();
    }
    
    @Test
    public void testLockingWithoutNodeId() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
        
        exception.expect(IllegalArgumentException.class);
        lockManager.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testLockingWithoutMode() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        
        exception.expect(IllegalArgumentException.class);
        lockManager.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testLockingWithoutQuorumKey() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
        conf.clear();
        
        exception.expect(IllegalArgumentException.class);
        lockManager.run(portBindings, conf, parameters);
    }

    @Test
    public void testLockingWithInvalidQuorumKey() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
        conf.set(ZKFailoverController.ZK_QUORUM_KEY, "invalid");
        
        exception.expect(IllegalArgumentException.class);
        lockManager.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testLockingWithUnsupportedMode() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, "unsupported");
        
        exception.expect(IllegalArgumentException.class);
        lockManager.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testLockingAndUnlocking() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid");
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());

        lockManager.run(portBindings, conf, parameters);

        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.release.name());
        lockManager.run(portBindings, conf, parameters);
        
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
        lockManager.run(portBindings, conf, parameters);

    }
    
    @Test
    public void testLockingOnDifferentNodes() throws Exception {
        for (int i=0; i < 10; i++) {
            Map<String, String> parameters = new HashMap<>();
            parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.obtain.name());
            parameters.put(LockManagingProcess.PARAM_NODE_ID, "nodeid_" + i);
            lockManager.run(portBindings, conf, parameters);
        }
    }
    
    @Test
    public void testUnlockingOnNotExistingNode() throws Exception {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(LockManagingProcess.PARAM_LOCK_MODE, LockMode.release.name());
        parameters.put(LockManagingProcess.PARAM_NODE_ID, "not_existing_nodeid");
        lockManager.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testAsyncLocking() throws Exception {
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
    }
    
}
