package eu.dnetlib.iis.common.java.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Util class containing operations on hdfs or local filesystem
 * 
 * @author madryk
 */
public class HdfsUtils {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Removes file or directory (recursively) located under the specified pathname.
     */
    public static void remove(Configuration hadoopConf, String pathname) throws IOException {
        
        Path path = new Path(pathname);
        
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        
    }
    
}
