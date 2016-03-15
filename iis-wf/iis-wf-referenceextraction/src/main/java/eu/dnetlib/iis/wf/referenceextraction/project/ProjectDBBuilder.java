package eu.dnetlib.iis.wf.referenceextraction.project;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonStreamWriter;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;

/**
 *
 * @author Dominika Tkaczyk
 */
public class ProjectDBBuilder implements Process {
    private final static String projectPort = "project";
	private final static String projectDBPort = "project_db";
	
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
		inputPorts.put(projectPort, new AvroPortType(Project.SCHEMA$));
		return inputPorts;
	}

	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = new HashMap<String, PortType>();
		outputPorts.put(projectDBPort, new AnyPortType());
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws IOException, InterruptedException {
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(conf);
		
		String targetDbLocation = System.getProperty("java.io.tmpdir") + 
				File.separatorChar + "base_projects.db";
		File targetDbFile = new File(targetDbLocation);
		FileUtils.copyFile(	new File("scripts/base_projects.db"), targetDbFile);
		targetDbFile.setWritable(true);
		
        java.lang.Process process = Runtime.getRuntime().exec(
                "python scripts/madis/mexec.py -d " + targetDbLocation + " -f scripts/buildprojectdb.sql");
        BufferedOutputStream stdin = new BufferedOutputStream(process.getOutputStream());
        InputStream errorStream = process.getErrorStream();
    
        Iterator<Project> projects = DataStore.getReader(new FileSystemPath(fs, input.get(projectPort)));

        JsonStreamWriter<Project> writer = 
                new JsonStreamWriter<Project>(Project.SCHEMA$, stdin);
        
        try {
	        while (projects.hasNext()) {
	            writer.write(projects.next());
	        }
	        writer.close();
	        process.waitFor();
        } catch (Exception e) {
//        	providing error details from Madis error stream
        	BufferedReader stderr = new BufferedReader(new InputStreamReader(errorStream));
            StringBuilder errorBuilder = new StringBuilder();
            String line;
            while ((line = stderr.readLine()) != null) {
                errorBuilder.append(line);
            }
            stderr.close();
            throw new IOException("got error while writing to Madis stream: " + 
            		errorBuilder.toString(), e);
        }
        
        if (process.exitValue() != 0) {
        	BufferedReader stderr = new BufferedReader(new InputStreamReader(errorStream));
            StringBuilder errorBuilder = new StringBuilder();
            String line;
            while ((line = stderr.readLine()) != null) {
                errorBuilder.append(line);
            }
            stderr.close();
            throw new RuntimeException("MadIS execution failed with error: " + errorBuilder.toString());
        }
        
        InputStream inStream = null;
        OutputStream outStream = null;
        try {
            inStream = new FileInputStream(targetDbFile);
            outStream = fs.create(new FileSystemPath(fs, output.get(projectDBPort)).getPath());
            IOUtils.copyStream(inStream, outStream);  
        } finally {
            if (inStream != null) {
                inStream.close();
            }
            if (outStream != null) {
                outStream.close();
            }
        }
	}
}
