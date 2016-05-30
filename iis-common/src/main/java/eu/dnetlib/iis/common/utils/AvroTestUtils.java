package eu.dnetlib.iis.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;

/**
 * @author Łukasz Dumiszewski
 */

public class AvroTestUtils {

    
    //------------------------ LOGIC --------------------------
    
    /**
     * Reads records from avro files from local filesystem
     */
    public static <T> List<T> readLocalAvroDataStore(String outputDirPath) throws IOException {
        Path outputPath = new Path(new File(outputDirPath).getAbsolutePath());
        
        FileSystem fs = createLocalFileSystem();
        
        List<T> records = DataStore.read(new FileSystemPath(fs, outputPath));
        return records;
    }


    /**
     * Creates directory and saves in it the passed objects (in avro files).<br/>
     * Passed object list cannot be empty (use {@link #createLocalAvroDataStore(List, String, Class)}
     * for empty lists).
     */
    public static <T extends GenericContainer> void createLocalAvroDataStore(List<T> records, String inputDirPath) throws IOException {
        
        File inputDir = new File(inputDirPath);
        inputDir.mkdir();
        Path inputPath = new Path(inputDir.getAbsolutePath());
        
        
        FileSystem fs = createLocalFileSystem();
        
        DataStore.create(records, new FileSystemPath(fs, inputPath));
        
    }
    
    /**
     * Creates directory and saves in it the passed objects (in avro files).<br/>
     */
    public static <T extends GenericContainer> void createLocalAvroDataStore(List<T> records, String inputDirPath, Class<T> recordsClass) throws IOException {
        

        File inputDir = new File(inputDirPath);
        inputDir.mkdir();
        Path inputPath = new Path(inputDir.getAbsolutePath());
        
        
        FileSystem fs = createLocalFileSystem();
        
        DataStore.create(records, new FileSystemPath(fs, inputPath), AvroUtils.toSchema(recordsClass.getName()));
        
    }
    
    
    //------------------------ PRIVATE --------------------------

    private static FileSystem createLocalFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        FileSystem fs = FileSystem.get(conf);
        return fs;
    }
}
