package eu.dnetlib.iis.common.lock;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ZKFailoverController;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.base.Preconditions;

/**
 * Zookeeper based lock manager factory.
 * 
 * @author mhorst
 *
 */
public class ZookeeperLockManagerFactory implements LockManagerFactory {

    
    public static final String DEFAULT_ROOT_NODE = "/cache";
    
    public static final String PARAM_ZK_SESSION_TIMEOUT = "zk_session_timeout";
    
    public static final int DEFAULT_SESSION_TIMEOUT = 60000;
    
    public static final Logger log = Logger.getLogger(ZookeeperLockManagerFactory.class);
    
    @Override
    public LockManager instantiate(Configuration hadoopConf) {
        
        String zkConnectionString = hadoopConf.get(ZKFailoverController.ZK_QUORUM_KEY,
                System.getProperty(ZKFailoverController.ZK_QUORUM_KEY));
        
        Preconditions.checkArgument(StringUtils.isNotBlank(zkConnectionString), 
                "zookeeper quorum is unknown, invalid '%s' property value: %s", ZKFailoverController.ZK_QUORUM_KEY, zkConnectionString);
        
        try {
            final ZooKeeper zooKeeper = initializeZookeeper(zkConnectionString);

            return new LockManager() {
                
                @Override
                public void obtain(String hdfsDir) throws Exception {
                    LockManagingProcessUtils.obtain(zooKeeper,
                            LockManagingProcessUtils.generatePath(hdfsDir, DEFAULT_ROOT_NODE));
                }
                
                @Override
                public void release(String hdfsDir) throws Exception {
                    LockManagingProcessUtils.release(zooKeeper, 
                            LockManagingProcessUtils.generatePath(hdfsDir, DEFAULT_ROOT_NODE));
                }
            };

        } catch (Exception e) {
            throw new RuntimeException("unexpected exception while initializing zookeeper", e);
        }
    }
    
    // -------------------------------- PRIVATE ----------------------------------
    
    private ZooKeeper initializeZookeeper(String zkConnectionString) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(zkConnectionString, DEFAULT_SESSION_TIMEOUT, (e) -> {
            // we are not interested in generic events
        });

        if (zooKeeper.exists(DEFAULT_ROOT_NODE, false) == null) {
            log.info("initializing root node: " + DEFAULT_ROOT_NODE);
            try {
            	zooKeeper.create(DEFAULT_ROOT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            	log.info("root node initialized");
            } catch(NodeExistsException e) {
				log.warn("root node '" + DEFAULT_ROOT_NODE + "' was already created by different process");
			}
        }
        return zooKeeper;
    }
    
}
