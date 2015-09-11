package eu.dnetlib.iis.workflows.websiteusage.document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AnyPortType;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;

/**
 * Website usage DB builder.
 * Creates website usage database by invoking Madis.
 * @author mhorst
 */
public class WebsiteUsageDBBuilder implements Process {
    private final static String logsPort = "logs";
	private final static String websiteUsageDBPort = "website_usage_db";
	
//	private String localFsRootDir = System.getProperty("java.io.tmpdir");
	private String localFsRootDir = "/tmp";
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> inputPorts = new HashMap<String, PortType>();
		inputPorts.put(logsPort, new AvroPortType(Project.SCHEMA$));
		return inputPorts;
	}

	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = new HashMap<String, PortType>();
		outputPorts.put(websiteUsageDBPort, new AnyPortType());
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws IOException, InterruptedException {
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();

		FileSystem fs = FileSystem.get(conf);
//		copying logs from HDFS location to local filesystem into temprorary directory
		File rootInputDir = new File(new File(localFsRootDir), 
				"website_logs_" + System.currentTimeMillis());
		if (!rootInputDir.mkdirs()) {
			throw new IOException("unable to create directory " +
					"in local FS: " + rootInputDir);
		}

		Path hdfsInput = input.get(logsPort);
		File localTargetLoc = new File(rootInputDir, hdfsInput.getName());
		FileUtil.copy(fs, hdfsInput, localTargetLoc, 
				false, conf);
		if (fs.exists(hdfsInput)) {
			java.lang.Process process = Runtime.getRuntime().exec(
//	              "python scripts/madis/mexec.py -d scripts/base_projects.db -f scripts/buildprojectdb.sql");
//			temporarily using dummy script
	        		"sh scripts/mock/build_website_usage_db.sh " + 
	        		localTargetLoc.getAbsolutePath() + 
	        		" scripts/websiteusage.db");
	        InputStream errorStream = process.getErrorStream();
	        process.waitFor();
	        
//	      	removing temporary directory
	        FileUtils.deleteDirectory(rootInputDir);
	        
//	      	processing errors, if any
	        BufferedReader stderr = new BufferedReader(new InputStreamReader(errorStream));
	        StringBuilder errorBuilder = new StringBuilder();
	        String line;
	        while ((line = stderr.readLine()) != null) {
	            errorBuilder.append(line);
	        }
	        stderr.close();
	        if (process.exitValue() != 0) {
	            throw new RuntimeException("MadIS execution failed with error: " + errorBuilder.toString());
	        }
	        
//	      	uploading created database to cluster
	        InputStream inStream = null;
	        OutputStream outStream = null;
	        try {
	            inStream = new FileInputStream(new File("scripts/websiteusage.db"));
	            outStream = fs.create(new FileSystemPath(fs, output.get(websiteUsageDBPort)).getPath());
	            IOUtils.copyStream(inStream, outStream);  
	        } finally {
	            if (inStream != null) {
	                inStream.close();
	            }
	            if (outStream != null) {
	                outStream.close();
	            }
	        }	
		} else {
			throw new IOException("input path " + input.get(logsPort) + " does not exist!");
		}
	}
}

