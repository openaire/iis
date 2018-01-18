package eu.dnetlib.iis.common.lock;

import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Zookeeper lock managing process. Blocks until lock is released.
 * 
 * @author mhorst
 *
 */
public class LockManagingProcess implements eu.dnetlib.iis.common.java.Process {

	public static final String DEFAULT_ROOT_NODE = "/cache";
	
	public static final String PARAM_ZK_SESSION_TIMEOUT = "zk_session_timeout";
	
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
	    
		Preconditions.checkArgument(parameters.containsKey(PARAM_NODE_ID), "node id not provided!");
		Preconditions.checkArgument(parameters.containsKey(PARAM_LOCK_MODE), "lock mode not provided!");

		String zkConnectionString = conf.get(ZKFailoverController.ZK_QUORUM_KEY);
		Preconditions.checkArgument(StringUtils.isNotBlank(zkConnectionString), 
		        "zookeeper quorum is unknown, invalid '%s' property value: %s", ZKFailoverController.ZK_QUORUM_KEY, zkConnectionString);

		int sessionTimeout = parameters.containsKey(PARAM_ZK_SESSION_TIMEOUT)?
		        Integer.valueOf(parameters.get(PARAM_ZK_SESSION_TIMEOUT)) : DEFAULT_SESSION_TIMEOUT;

		final ZooKeeper zooKeeper = new ZooKeeper(zkConnectionString, sessionTimeout, (e) -> {
		 // we are not interested in generic events
		});
		
//		initializing root node if does not exist
		if (zooKeeper.exists(DEFAULT_ROOT_NODE, false) == null) {
			log.info("initializing root node: " + DEFAULT_ROOT_NODE);
			zooKeeper.create(DEFAULT_ROOT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			log.info("root node initialized");
		}

		final String nodePath = LockManagingProcessUtils.generatePath(parameters.get(PARAM_NODE_ID), DEFAULT_ROOT_NODE);
		
		final Semaphore semaphore = new Semaphore(1);
		semaphore.acquire();
		
		switch(LockMode.valueOf(parameters.get(PARAM_LOCK_MODE))) {
		    case obtain: {
		        LockManagingProcessUtils.obtain(zooKeeper, nodePath, semaphore);
		        break;
		    }
		    case release: {
		        LockManagingProcessUtils.release(zooKeeper, nodePath);
		        break;
		    }
		    default: {
		        throw new InvalidParameterException("unsupported lock mode: " + parameters.get(PARAM_LOCK_MODE));
		    }
		}
	}
	
}
