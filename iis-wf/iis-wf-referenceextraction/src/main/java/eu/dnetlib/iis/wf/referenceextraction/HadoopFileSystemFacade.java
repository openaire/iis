package eu.dnetlib.iis.wf.referenceextraction;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
    public OutputStream create(Path f) throws IOException {
        return fs.create(f);
    }

    @Override
    public FileSystem getFileSystem() {
        return fs;
    }

}
