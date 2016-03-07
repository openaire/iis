package eu.dnetlib.iis.core.examples.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/** Copies text files line by line
 * @author Mateusz Kobos
 */
public class LineByLineCopier implements Process {
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return createInputPorts();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createInputPorts(){
		HashMap<String, PortType> inputPorts = 
				new HashMap<String, PortType>();
		inputPorts.put("person", new AnyPortType());
		inputPorts.put("document", new AnyPortType());
		return inputPorts;
	}
	
	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put("person_copy", new AnyPortType());
		outputPorts.put("document_copy", new AnyPortType());
		return outputPorts;	
	}
	
	/**
	 * Copy input files to output
	 */
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> input = portBindings.getInput();
		Map<String, Path> output = portBindings.getOutput();
		FileSystem fs = FileSystem.get(conf);
		copy(fs, input.get("person"), output.get("person_copy"));
		copy(fs, input.get("document"), output.get("document_copy"));
	}
	
	private void copy(FileSystem fs, Path src, Path dst) throws IOException{
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
        	FSDataInputStream inStream = fs.open(src);
        	FSDataOutputStream outStream = fs.create(dst);
            reader = new BufferedReader(new InputStreamReader(inStream));
            writer = new BufferedWriter(new OutputStreamWriter(outStream));
            for(String line = reader.readLine(); line != null; 
                    line = reader.readLine()){
//            	if(firstPass){
//            		firstPass = false;
//            	} else {
//            		writer.write("\n");
//            	}
//                writer.write(line);
            	writer.write(line+"\n");
            }
        }
        finally {
            if(reader != null)
                reader.close();
            if(writer != null)
            	writer.close();
        }
	}
}
