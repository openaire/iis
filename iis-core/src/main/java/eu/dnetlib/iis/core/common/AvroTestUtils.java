package eu.dnetlib.iis.core.common;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;

/**
 * @author Łukasz Dumiszewski
 */

public class AvroTestUtils {

    
    public static <T> List<T> readLocalAvroDataStore(String outputDirPath) throws IOException {
        Path outputPath = new Path(new File(outputDirPath).getAbsolutePath());
        
        FileSystem fs = createLocalFileSystem();
        
        List<T> people = DataStore.read(new FileSystemPath(fs, outputPath));
        return people;
    }


    public static <T extends GenericContainer> void createLocalAvroDataStore(List<T> objects, String inputDirPath) throws IOException {
        
        File inputDir = new File(inputDirPath);
        inputDir.mkdir();
        Path inputPath = new Path(inputDir.getAbsolutePath());
        
        
        FileSystem fs = createLocalFileSystem();
        
        DataStore.create(objects, new FileSystemPath(fs, inputPath));
        
    }
    

    private static FileSystem createLocalFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        FileSystem fs = FileSystem.get(conf);
        return fs;
    }
}
