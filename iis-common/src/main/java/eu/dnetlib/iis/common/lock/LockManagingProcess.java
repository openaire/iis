package eu.dnetlib.iis.common.lock;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Zookeeper lock managing process.
 * Blocks until lock is released.
 * @author mhorst
 *
 */
public class LockManagingProcess implements eu.dnetlib.iis.common.java.Process {

	public static final String DEFAULT_ROOT_NODE = "/cache";
	
	public static final String NODE_SEPARATOR = "/";
	
	public static final String PARAM_ZK_SESSION_TIMEOUT = "zk_session_timeout";
	
	public static final String PARAM_ROOT_NODE = "root_node";
	
	public static final String PARAM_NODE_ID = "node_id";
	
	public static final String PARAM_LOCK_MODE = "mode";
	
	public static enum LockMode {
		obtain,
		release
	}
	
	public static final int DEFAULT_SESSION_TIMEOUT = 60000;
	
	public static final Logger log = Logger.getLogger(LockManagingProcess.class);
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		
		if (!parameters.containsKey(PARAM_NODE_ID)) {
			throw new Exception("node id not provided!");
		} 
		if (!parameters.containsKey(PARAM_LOCK_MODE)) {
			throw new Exception("lock mode not provided!");
		}

		String zkConnectionString = conf.get(
				ZKFailoverController.ZK_QUORUM_KEY);
		if (zkConnectionString==null || zkConnectionString.isEmpty()) {
			throw new Exception("zookeeper quorum is unknown, invalid " + 
					ZKFailoverController.ZK_QUORUM_KEY + 
					" property value: " + zkConnectionString);
		}
		int sessionTimeout;
		if (parameters.containsKey(PARAM_ZK_SESSION_TIMEOUT)) {
			sessionTimeout = Integer.valueOf(parameters.get(PARAM_ZK_SESSION_TIMEOUT));
		} else {
			sessionTimeout = DEFAULT_SESSION_TIMEOUT;
		}

		final ZooKeeper zk = new ZooKeeper(zkConnectionString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
			}
		});

		String rootNode = parameters.containsKey(PARAM_ROOT_NODE)?
				parameters.get(PARAM_ROOT_NODE):
					DEFAULT_ROOT_NODE;
		
//		initializing root node if does not exist
		if (zk.exists(rootNode, false)==null) {
			log.warn("initializing root node: " + rootNode);
			zk.create(rootNode, 
					 new byte[0], 
					 ZooDefs.Ids.OPEN_ACL_UNSAFE, 
					 CreateMode.PERSISTENT);
			log.warn("root node initialized");
		}

		final String nodePath = generatePath(
				parameters.get(PARAM_NODE_ID), rootNode);
		
		LockMode lockMode = LockMode.valueOf(parameters.get(PARAM_LOCK_MODE));
		
		final Semaphore semaphore = new Semaphore(1);
		semaphore.acquire();
		
		if (LockMode.obtain.equals(lockMode)) {
			log.warn("trying to obtain lock: " + nodePath);
			if (zk.exists(nodePath, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					 if (Event.EventType.NodeDeleted == event.getType()) {
                         log.warn(nodePath + " lock release detected");
                         log.warn("creating new lock instance: " + nodePath + "...");
                         try {
							zk.create(nodePath, 
									 new byte[0], 
									 ZooDefs.Ids.OPEN_ACL_UNSAFE, 
									 CreateMode.PERSISTENT);
							log.warn("lock" + nodePath + " created");
							semaphore.release();
							
						} catch (KeeperException e) {
							throw new RuntimeException(e);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
                     }
				}
			})==null) {
				log.warn("lock not found, creating new lock instance: " + nodePath);
				zk.create(nodePath, new byte[0], 
						ZooDefs.Ids.OPEN_ACL_UNSAFE, 
               		 	CreateMode.PERSISTENT);
				log.warn("lock" + nodePath + " created");	
				semaphore.release();
			} else {
//				waiting until node is removed by other lock manager
				log.warn("waiting until lock is released");
				long startTime = System.currentTimeMillis();
				semaphore.acquire();
				log.warn("lock released, waited for " + 
						(System.currentTimeMillis()-startTime) + " ms");
				semaphore.release();
			}
		} else if (LockMode.release.equals(lockMode)) {
			log.warn("removing lock" + nodePath + "...");
			zk.delete(nodePath, -1);
			log.warn("lock" + nodePath + " removed");
		} else {
			throw new Exception("unsupported lock mode: " + lockMode);
		}
	}
	
	public static final String generatePath(String nodeId, String rootNode) {
		if (nodeId!=null) {
			return rootNode + NODE_SEPARATOR + nodeId.replace('/', '_');
		} else {
			return null;
		}
	}
	
}
