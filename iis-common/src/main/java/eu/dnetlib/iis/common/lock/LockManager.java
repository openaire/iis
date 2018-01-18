package eu.dnetlib.iis.common.lock;

/**
 * Lock manager interface.
 * 
 * @author mhorst
 *
 */
public interface LockManager {

    /**
     * Obtains lock for given HDFS directory.
     */
    void obtain(String hdfsDir) throws Exception;
    
    /**
     * Releases lock for given HDFS directory.
     */
    void release(String hdfsDir) throws Exception;

}
