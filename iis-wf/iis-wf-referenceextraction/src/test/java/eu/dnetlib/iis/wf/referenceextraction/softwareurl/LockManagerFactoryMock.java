package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.lock.LockManagerFactory;

/**
 * Mock implementation of lock manager.
 * 
 * @author mhorst
 *
 */
public class LockManagerFactoryMock implements LockManagerFactory {


    @Override
    public LockManager instantiate(Configuration hadoopConf) {
        return new LockManager() {
            
            @Override
            public void release(String hdfsDir) throws Exception {
            }
            
            @Override
            public void obtain(String hdfsDir) throws Exception {
            }
        };
    }

}
