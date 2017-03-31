package eu.dnetlib.iis.common.cache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.FsShellPermissions;
import eu.dnetlib.iis.common.FsShellPermissions.Op;

/**
 * {@link FileSystem} based facade. 
 * 
 * @author mhorst
 *
 */
public class HadoopFileSystemFacade implements FileSystemFacade {

    private final FileSystem fs;
    
    // ------------------------------- CONSTRUCTORS ----------------------
    
    public HadoopFileSystemFacade(FileSystem fs) throws IOException {
        this.fs = fs;
    }

    // ------------------------------- LOGIC -----------------------------
    
    @Override
    public boolean exists(Path path) throws IOException {
        return fs.exists(path);
    }

    @Override
    public InputStream open(Path f) throws IOException {
        return fs.open(f);
    }

    @Override
    public OutputStream create(Path f, boolean overwrite) throws IOException {
        return fs.create(f, overwrite);
    }

    @Override
    public void changePermissions(Configuration config, Op op, boolean recursive, String group, String uri) {
        FsShellPermissions.changePermissions(this.fs, config, op, recursive, group, uri);

    }

}
