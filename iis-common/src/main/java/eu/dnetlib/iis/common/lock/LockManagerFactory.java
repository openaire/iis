package eu.dnetlib.iis.common.lock;

import org.apache.hadoop.conf.Configuration;

/**
 * Lock manager factory.
 * 
 * @author mhorst
 *
 */
public interface LockManagerFactory {

    /**
     * Instantiates lock manager.
     * @param hadoopConf hadoop configuration
     */
    LockManager instantiate(Configuration hadoopConf);
    
}
