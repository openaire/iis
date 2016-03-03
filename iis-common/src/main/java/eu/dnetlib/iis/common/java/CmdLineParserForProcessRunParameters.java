package eu.dnetlib.iis.common.java;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;

/**
 * Handles parsing parameters passed to the {@link Process}
 * @author Mateusz Kobos
 *
 */
public class CmdLineParserForProcessRunParameters {
	/** Parse the command line arguments.
	 * 
	 * @param cmdLine command line arguments
	 * @param ports names of ports that ought to be extracted from command line
	 */
	public ProcessParameters run(CommandLine cmdLine, Ports ports) {

		Properties inputProperties = cmdLine.getOptionProperties(
				CmdLineParser.inputPrefix);
		assumePortNamesMatch(CmdLineParser.inputPrefix, inputProperties, 
				ports.getInput().keySet());
		Map<String, Path> inputBindings = getBindings(
				inputProperties, ports.getInput().keySet());

		Properties outputProperties = cmdLine.getOptionProperties(
				CmdLineParser.outputPrefix);
		assumePortNamesMatch(CmdLineParser.outputPrefix, outputProperties, 
				ports.getOutput().keySet());
		Map<String, Path> outputBindings = getBindings(
				outputProperties, ports.getOutput().keySet());

		PortBindings bindings = new PortBindings(inputBindings, outputBindings);

		Properties specialProperties = cmdLine.getOptionProperties(
				CmdLineParser.specialParametersPrefix);
		assumeContainAllMandatoryParameters(
				specialProperties, CmdLineParser.mandatorySpecialParameters);

		Properties rawProperties = cmdLine.getOptionProperties(
				CmdLineParser.processParametersPrefix);
		Map<String, String> processParameters = new HashMap<String, String>();
		for(Entry<Object, Object> entry: rawProperties.entrySet()){
			processParameters.put(
					(String)entry.getKey(),	(String)entry.getValue());
		}
		
		return new ProcessParameters(bindings, processParameters);
	}
	
	private static void assumeContainAllMandatoryParameters(
			Properties properties, String[] mandatoryParameters){
		for(String otherParameter: mandatoryParameters){
			if(!properties.containsKey(otherParameter)){
				throw new CmdLineParserException(String.format(
						"Not all mandatory properties are set using the \"%s\" "
						+ "option are given, e.g. \"-%s\" parameter is missing",
						CmdLineParser.specialParametersPrefix, otherParameter));
			}
		}
		return;
	}
	
	private static void assumePortNamesMatch(String cmdLineParamPrefix,
			Properties cmdLineProperties, Set<String> portNames) {
		for (String name : portNames) {
			if (!cmdLineProperties.containsKey(name)) {
				throw new CmdLineParserException(String.format(
					"The port with name \"%s\" is not specified in "
					+ "command line (command line option \"-%s\" is missing)",
					name, cmdLineParamPrefix + name));
			}
		}
		for (Object cmdLineKeyObject : cmdLineProperties.keySet()) {
			String name = (String) cmdLineKeyObject;
			if (!portNames.contains(name)) {
				throw new CmdLineParserException(String.format(
						"A port name \"%s\" which is not specified is given "
						+ "in the command line "
						+ "(command line option \"%s\" is excess)",
						name, cmdLineParamPrefix + name));
			}
		}
	}

	private static Map<String, Path> getBindings(
			Properties cmdLineProperties, Set<String> portNames) {
		Map<String, Path> bindings = new HashMap<String, Path>();
		for (String name : portNames) {
			Path path = new Path((String) cmdLineProperties.get(name));
			bindings.put(name, path);
		}
		return bindings;
	}
}
