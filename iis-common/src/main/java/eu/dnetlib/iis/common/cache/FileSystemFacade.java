package eu.dnetlib.iis.common.cache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.FsShellPermissions.Op;

/**
 * File system facade introducing abstraction layer when communicating with file system.
 * 
 * @author mhorst
 *
 */
public interface FileSystemFacade {

    /**
     * Checks whether given path exists.
     */
    boolean exists(Path path) throws IOException;
    
    /**
     * Opens {@link InputStream} for given location.
     */
    InputStream open(Path f) throws IOException;
    
    /**
     * Creates {@link OutputStream} for given location. Overwrites existing file when flag set.
     */
    OutputStream create(Path f, boolean overwrite) throws IOException;
    
    /**
     * Changes permissions for given uri.
     */
    void changePermissions(Configuration config, Op op, boolean recursive, String group, String uri);
    
}
