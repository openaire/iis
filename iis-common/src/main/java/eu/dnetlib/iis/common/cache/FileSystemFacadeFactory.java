package eu.dnetlib.iis.common.cache;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * {@link FileSystemFacade} factory.
 * @author mhorst
 *
 */
@FunctionalInterface
public interface FileSystemFacadeFactory {

    /**
     * Creates facade instance based on given configuration.
     */
    FileSystemFacade create(Configuration conf) throws IOException;
    
}
