package eu.dnetlib.iis.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

import com.google.common.collect.Maps;

import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;

/**
 * Class containing operations on hdfs filesystem used in tests
 * 
 * @author madryk
 *
 */
class HdfsTestHelper {

	private FileSystem hadoopFilesystem;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public HdfsTestHelper(FileSystem hadoopFilesystem) {
		this.hadoopFilesystem = hadoopFilesystem;
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Copies files from hdfs to local filesystem
	 * 
	 * @param basePath - common path in hdfs for all files in filePaths parameter
	 * @param filesPaths - relative to provided basePath
	 * @param targetDir - directory where imported files will be stored
	 * @return map with entries in form {original path from hdfs; corresponding imported files}
	 */
	public Map<String, File> copyFilesFromHdfs(String basePath, List<String> filesPaths, File targetDir) {
		Map<String, File> importedFiles = Maps.newHashMap();
		
		for (String filePath : filesPaths) {
			String filename = new File(filePath).getName();

			try {
				hadoopFilesystem.copyToLocalFile(new Path(basePath, filePath), new Path(targetDir.getAbsolutePath(), filename));
			} catch (FileNotFoundException e) {
				Assert.fail("Expected file: " + filePath + " has not been found");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			importedFiles.put(filePath, new File(targetDir, filename));
		}
		
		return importedFiles;
	}
	
	/**
	 * Reads avro datastores from hdfs filesystem
	 * 
	 * @param basePath - common path in hdfs for all datastores in datastoresPaths parameter
	 * @param datastoresPaths - relative to provided basePath
	 * @return map with entries in form {original path from hdfs; corresponding datastore}
	 */
	public Map<String, List<? extends SpecificRecord>> readAvroDatastoresFromHdfs(String basePath, List<String> datastoresPaths) {
		Map<String, List<? extends SpecificRecord>> importedFiles = Maps.newHashMap();
		
		for (String datastorePath : datastoresPaths) {
			List<SpecificRecord> dataStore = null;
			
			try {
				dataStore = DataStore.read(new FileSystemPath(hadoopFilesystem, new Path(basePath, datastorePath)));
			} catch (FileNotFoundException e) {
				Assert.fail("Expected datastore: " + datastorePath + " has not been found");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			importedFiles.put(datastorePath, dataStore);
		}
		
		return importedFiles;
	}
}
