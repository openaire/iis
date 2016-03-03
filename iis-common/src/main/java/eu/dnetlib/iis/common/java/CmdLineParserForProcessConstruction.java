package eu.dnetlib.iis.common.java;

import java.lang.reflect.Constructor;

import org.apache.commons.cli.CommandLine;

/**
 * Handles parsing the command line arguments provided by the Oozie
 * to create a {@link Process}
 * @author Mateusz Kobos
 *
 */
public class CmdLineParserForProcessConstruction {
	public Process run(CommandLine cmdLine){
		String[] args = cmdLine.getArgs();
		if(args.length != 1){
			throw new CmdLineParserException("The name of the class has "+
					"to be specified as the first agrument");
		}
		String className = args[0];
		
		String[] constructorParams = cmdLine.getOptionValues(
				CmdLineParser.constructorPrefix);
		if(constructorParams == null){
			constructorParams = new String[0];
		}
		try {
			Class<?> processClass = Class.forName(className);
			Constructor<?> processConstructor = null;
			if(constructorParams.length == 0){
				try{
					processConstructor = processClass.getConstructor();
					return (Process) processConstructor.newInstance();
				} catch(NoSuchMethodException ex){
				}
			}
			processConstructor = processClass.getConstructor(String[].class);
			return (Process) processConstructor.newInstance(
						(Object)constructorParams);
		} catch (Exception e) {
			throw new CmdLineParserException(String.format(
					"Problem while creating class \"%s\"", className), e);
		}
	}
}
