package eu.dnetlib.iis.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;

/**
 * Class containing operations on hdfs filesystem used in tests
 * 
 * @author madryk
 *
 */
class HdfsTestHelper {
	
	private SshHdfsFileFetcher hdfsFileFetcher;
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	public HdfsTestHelper(SshHdfsFileFetcher hdfsFileFetcher) {
		this.hdfsFileFetcher = hdfsFileFetcher;
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
		Map<String, File> copiedFiles = Maps.newHashMap();
		
		for (String filePath : filesPaths) {
			File copiedFile = null;
			
			try {
				copiedFile = hdfsFileFetcher.fetchFile(basePath + "/" + filePath, targetDir);
			} catch (FileNotFoundException e) {
				Assert.fail("Expected file: " + filePath + " has not been found");
			}

			copiedFiles.put(filePath, copiedFile);
		}
		
		return copiedFiles;
	}
	
	/**
	 * Reads avro datastores from hdfs filesystem
	 * 
	 * @param basePath - common path in hdfs for all datastores in datastoresPaths parameter
	 * @param datastoresPaths - relative to provided basePath
	 * @return map with entries in form {original path from hdfs; corresponding datastore}
	 */
	public Map<String, List<? extends SpecificRecord>> readAvroDatastoresFromHdfs(String basePath, List<String> datastoresPaths) {
		Map<String, List<? extends SpecificRecord>> importedDatastores = Maps.newHashMap();
		
		for (String datastorePath : datastoresPaths) {
			List<SpecificRecord> dataStore = null;
			File targetDir = Files.createTempDir();
			
			try {
				File localDatastorePath = hdfsFileFetcher.fetchFile(basePath + "/" + datastorePath, targetDir);
				
				dataStore = DataStore.read(new FileSystemPath(localDatastorePath));
				
			} catch (FileNotFoundException e) {
				Assert.fail("Expected datastore: " + datastorePath + " has not been found");
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				FileUtils.deleteQuietly(targetDir);
			}
			
			importedDatastores.put(datastorePath, dataStore);
		}
		
		return importedDatastores;
	}
}
