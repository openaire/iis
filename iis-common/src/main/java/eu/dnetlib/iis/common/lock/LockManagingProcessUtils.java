package eu.dnetlib.iis.common.lock;

import java.util.Date;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

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
    public static void obtain(final ZooKeeper zooKeeper, final String nodePath) throws KeeperException, InterruptedException {
        log.info("trying to obtain lock: " + nodePath);

    	//acquiring to wait in case of lock being obtained by different job
    	final Semaphore semaphore = new Semaphore(1);
		semaphore.acquire();
		
        if (zooKeeper.exists(nodePath, (event) -> {
        	handleEvent(zooKeeper, nodePath, semaphore, event);
        }) == null) {
        	
        	log.info("lock not found, creating new lock");
        	createLock(zooKeeper, nodePath, semaphore);
        	
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
        try {
        	zooKeeper.delete(nodePath, -1);
        	log.info("lock" + nodePath + " removed");
        } catch (NoNodeException e) {
        	log.error("unable to remove non existing node:" + nodePath);
        }
    }
    
    // ------------------------------------- PRIVATE ----------------------------------------------
    
    private static void handleEvent(final ZooKeeper zooKeeper, final String nodePath, final Semaphore semaphore, WatchedEvent event) {
    	if (Event.EventType.NodeDeleted == event.getType()) {
            log.info(nodePath + " lock release detected, trying to create new lock");
            createLock(zooKeeper, nodePath, semaphore);
        }
    }
    
    private static void createLock(final ZooKeeper zooKeeper, final String nodePath, final Semaphore semaphore) {
		try {
			log.info("creating new lock instance: " + nodePath);
			zooKeeper.create(nodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			log.info("lock" + nodePath + " created");
			semaphore.release();
		} catch (NodeExistsException e) {
			try {
				log.info("lock " + nodePath + " already obtained by different job, awaiting lock release");
				Stat stat = zooKeeper.exists(nodePath, (event) -> {
					handleEvent(zooKeeper, nodePath, semaphore, event);
				});
				if (stat != null) {
					log.info("already obtained lock creation time: " + new Date(stat.getCtime()));
				}
			} catch (KeeperException | InterruptedException e1) {
				throw new RuntimeException(e);
			}
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
    }
    
    /**
     * Generates zookeeper node path based on HDFS directory location and root node.
     */
    protected static final String generatePath(String hdfsDir, String rootNode) {
        return rootNode + NODE_SEPARATOR + hdfsDir.replace('/', '_');
    }

}
