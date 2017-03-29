package eu.dnetlib.iis.common.java;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

/**
 * 
 * @author Mateusz Kobos
 *
 */
@SuppressWarnings("deprecation")
public final class CmdLineParser {
	/** HACK: make the names of various types of parameters of the program 
	* more readable, e.g. "--Input_person=..." instead of "-Iperson=...",
	* "--Output_merged=..." instead of "-Omerged=...". I wasn't able to
	* get such notation so far using the Apache CLI. */
	public static final String constructorPrefix = "C";
	public static final String inputPrefix = "I";
	public static final String outputPrefix = "O";
	public static final String specialParametersPrefix = "S";
	/** HACK: This field should be removed since this list of special 
	 * parameters is empty, thus not used anywhere.*/
	public static final String[] mandatorySpecialParameters = new String[]{};
	public static final String processParametersPrefix = "P";
	
	// ------------------------- CONSTRUCTORS ------------------------------
	
	private CmdLineParser() {}
	
	// ------------------------- LOGIC -------------------------------------
	
    public static CommandLine parse(String[] args) {
		Options options = new Options();
		@SuppressWarnings("static-access")
		Option constructorParams = OptionBuilder.withArgName("STRING")
				.hasArg()
				.withDescription("Constructor parameter")
				.withLongOpt("ConstructorParam")
				.create(constructorPrefix);
		options.addOption(constructorParams);
		@SuppressWarnings("static-access")
		Option inputs = OptionBuilder.withArgName("portName=URI")
				.hasArgs(2)
				.withValueSeparator()
				.withDescription("Path binding for a given input port")
				.withLongOpt("Input")
				.create(inputPrefix);
		options.addOption(inputs);
		@SuppressWarnings("static-access")
		Option outputs = OptionBuilder.withArgName("portName=URI")
				.hasArgs(2)
				.withValueSeparator()
				.withDescription("Path binding for a given output port")
				.create(outputPrefix);
		options.addOption(outputs);
		@SuppressWarnings("static-access")
		Option specialParameter = OptionBuilder.withArgName("parameter_name=string")
				.hasArgs(2)
				.withValueSeparator()
				.withDescription(String.format("Value of special parameter. "
						+ "These are the mandatory parameters={%s}",
						StringUtils.join(mandatorySpecialParameters, ",")))
				.create(specialParametersPrefix);
		options.addOption(specialParameter);
		@SuppressWarnings("static-access")
		Option otherParameter = OptionBuilder.withArgName("parameter_name=string")
				.hasArgs(2)
				.withValueSeparator()
				.withDescription(
						String.format("Value of some other parameter."))
				.create(processParametersPrefix);
		options.addOption(otherParameter);
		
		Option help = new Option("help", "print this message");
		options.addOption(help);

		CommandLineParser parser = new GnuParser();
		try {
			CommandLine cmdLine = parser.parse(options, args);
			if(cmdLine.hasOption("help")){
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("", options );
				System.exit(1);
			}
			return cmdLine;
		} catch (ParseException e) {
			throw new CmdLineParserException("Parsing command line arguments failed", e);
		}
		
	}
}
