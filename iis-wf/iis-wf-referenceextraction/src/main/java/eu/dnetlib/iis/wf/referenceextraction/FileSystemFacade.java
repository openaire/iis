package eu.dnetlib.iis.wf.referenceextraction;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * File system facade introducing abstraction layer when communicating with file system.
 * 
 * @author mhorst
 *
 */
public interface FileSystemFacade {
    
    /**
     * Creates {@link OutputStream} for given location. Overwrites existing file when flag set.
     */
    OutputStream create(Path f) throws IOException;
    
    /**
     * @return underlying file system
     */
    FileSystem getFileSystem();
}
