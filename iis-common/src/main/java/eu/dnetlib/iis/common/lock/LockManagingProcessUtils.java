package eu.dnetlib.iis.common.lock;

import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;

import com.google.common.base.Stopwatch;

/**
 * Lock managing process utility methods.
 * 
 * @author mhorst
 *
 */
public class LockManagingProcessUtils {

    private static final Logger log = Logger.getLogger(LockManagingProcessUtils.class);
    
    private static final String NODE_SEPARATOR = "/";
    
    
    /**
     * Obtains zookeeper lock.
     */
    public static void obtain(final ZooKeeper zooKeeper, final String nodePath, final Semaphore semaphore) throws KeeperException, InterruptedException {
        log.info("trying to obtain lock: " + nodePath);
        if (zooKeeper.exists(nodePath, (event) -> {
            if (Event.EventType.NodeDeleted == event.getType()) {
                try {
                    log.info(nodePath + " lock release detected");
                    log.info("creating new lock instance: " + nodePath + "...");
                    zooKeeper.create(nodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    log.info("lock" + nodePath + " created");
                    semaphore.release();
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }) == null) {
            log.info("lock not found, creating new lock instance: " + nodePath);
            zooKeeper.create(nodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("lock" + nodePath + " created");
            semaphore.release();
        } else {
            // waiting until node is removed by other lock manager
            log.info("waiting until lock is released");
            Stopwatch timer = new Stopwatch().start();
            semaphore.acquire();
            log.info("lock released, waited for " + timer.elapsedMillis() + " ms");
            semaphore.release();
        }
    }
    
    /**
     * Releases zookeeper lock.
     */
    public static void release(final ZooKeeper zooKeeper, final String nodePath) throws InterruptedException, KeeperException {
        log.info("removing lock" + nodePath + "...");
        zooKeeper.delete(nodePath, -1);
        log.info("lock" + nodePath + " removed");
    }
    
    /**
     * Generates zookeeper node path based on HDFS directory location and root node.
     */
    public static final String generatePath(String hdfsDir, String rootNode) {
        return rootNode + NODE_SEPARATOR + hdfsDir.replace('/', '_');
    }

}
