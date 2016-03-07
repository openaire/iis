package eu.dnetlib.iis.common;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;

import com.google.common.collect.Maps;

/**
 * Class containing results of test workflow.
 * It can be used for additional asserts.
 * 
 * @author madryk
 *
 */
public class WorkflowTestResult {

	private Map<String, File> workflowOutputFiles = Maps.newHashMap();
	
	private Map<String, List<? extends SpecificRecord>> workflowOutputAvroDataStores = Maps.newHashMap();

	
	/**
	 * Adds local copy of file from hdfs located under filepath parameter
	 * to test results
	 */
	public void addWorkflowOutputFiles(String filepath, File file) {
		workflowOutputFiles.put(filepath, file);
	}
	
	/**
	 * Returns local copy of file from hdfs located under filepath parameter
	 */
	public File getWorkflowOutputFile(String filepath) {
		return workflowOutputFiles.get(filepath);
	}
	
	public void setWorkflowOutputFiles(Map<String, File> workflowOutputFiles) {
		this.workflowOutputFiles = workflowOutputFiles;
	}

	/**
	 * Adds avro datastore located under path parameter to test results
	 */
	public void addAvroDataStore(String path, List<? extends SpecificRecord> avroDataStore) {
		this.workflowOutputAvroDataStores.put(path, avroDataStore);
	}
	
	/**
	 * Returns avro datastore located under path parameter
	 */
	public <T extends SpecificRecord> List<T> getAvroDataStore(String path) {
		List<T> avroDataStore = (List<T>) workflowOutputAvroDataStores.get(path);
		
		return avroDataStore;
	}
	
	public void setWorkflowOutputAvroDataStores(Map<String, List<? extends SpecificRecord>> workflowOutputAvroDataStores) {
		this.workflowOutputAvroDataStores = workflowOutputAvroDataStores;
	}
	
}
