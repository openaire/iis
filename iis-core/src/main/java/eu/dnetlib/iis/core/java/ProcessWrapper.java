package eu.dnetlib.iis.core.java;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;
import eu.dnetlib.iis.core.java.porttype.AvroPortType;
import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * Creates {@link Process} object through reflection by parsing 
 * the command-line arguments
 * @author Mateusz Kobos
 *
 */
public class ProcessWrapper {
	
	public Configuration getConfiguration() throws Exception{
		return new Configuration();
	}
	
	public static void main(String[] args) throws Exception {
		ProcessWrapper wrapper = new ProcessWrapper();
		wrapper.run(args);
	}
	
	public void run(String[] args) throws Exception{
		CommandLine cmdLine = CmdLineParser.parse(args);
		
		CmdLineParserForProcessConstruction constructionParser = 
				new CmdLineParserForProcessConstruction();
		Process process = constructionParser.run(cmdLine);
		Ports ports = 
				new Ports(process.getInputPorts(), process.getOutputPorts());
		CmdLineParserForProcessRunParameters runParametersParser =
				new CmdLineParserForProcessRunParameters();
		ProcessParameters params = runParametersParser.run(cmdLine, ports);
		Configuration conf = getConfiguration();
		process.run(params.getPortBindings(), conf, params.getParameters());
		createOutputsIfDontExist(
				process.getOutputPorts(), params.getPortBindings().getOutput(),
				conf);
	}
	
	private static void createOutputsIfDontExist(
			Map<String, PortType> outputPortsSpecification, 
			Map<String, Path> outputPortBindings, Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		for(Map.Entry<String, Path> entry: outputPortBindings.entrySet()){
			Path path = entry.getValue();
			if(!fs.exists(path) || isEmptyDirectory(fs, path)){
				PortType rawType = outputPortsSpecification.get(entry.getKey());
				if(!(rawType instanceof AvroPortType)){
					throw new RuntimeException("The port \""+entry.getKey()+
							"\" is not of Avro type and only Avro types are "+
							"supported");
				}
				AvroPortType type = (AvroPortType) rawType;
				FileSystemPath fsPath = new FileSystemPath(fs, path);
				DataFileWriter<GenericContainer> writer = 
						DataStore.create(fsPath, type.getSchema());
				writer.close();
			}
		}
	}
	
	private static boolean isEmptyDirectory(FileSystem fs, Path path) throws IOException{
		if(!fs.isDirectory(path)){
			return false;
		}
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, false);
		/** There's at least one file, so the directory is not empty */
		if(files.hasNext()){
			return false;
		}
		return true;
	}
}
