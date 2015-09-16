package eu.dnetlib.iis.core.examples.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;

import eu.dnetlib.iis.core.java.PortBindings;
import eu.dnetlib.iis.core.java.Process;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AnyPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;


public class StopwordsProducer implements Process {
	
	private final static String stopwordsPort = "stopwords";

    @Override
	public Map<String, PortType> getInputPorts() {
		return new HashMap<String, PortType>();
	}
	
	@Override
	public Map<String, PortType> getOutputPorts() {
		return createOutputPorts();
	}

	private static HashMap<String, PortType> createOutputPorts(){
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put(stopwordsPort, 
				new AnyPortType());
		return outputPorts;	
	}
	
	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters)	throws IOException{
		Map<String, Path> output = portBindings.getOutput();

        FileSystem fs = FileSystem.get(conf);
		InputStream inStream = IOUtils.getResourceAsStream("eu/dnetlib/iis/core/examples/data/stopwords.db", -1);
		OutputStream outStream = fs.create(new FileSystemPath(fs, output.get(stopwordsPort)).getPath());
		IOUtils.copyStream(inStream, outStream);
	}

}