package eu.dnetlib.iis.common.lock;

import org.apache.hadoop.conf.Configuration;
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
