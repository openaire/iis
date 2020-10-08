package eu.dnetlib.iis.common.java;

import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 
 * @author Mateusz Kobos
 *
 */
public class CmdLineParserForProcessRunParametersTest{

	@Test
	public void testBasic() throws URISyntaxException {
		CmdLineParserForProcessRunParameters parser = 
				new CmdLineParserForProcessRunParameters();
		String[] args = new String[]{
				"-Iperson=/users/joe/person_input",
				"-Idocument=/users/joe/doc_input",
				"-Omerged=/users/joe/merged_out",
				"-Page=33"};
		Ports ports = createStandardPorts();
		CommandLine cmdLine = CmdLineParser.parse(args);
		ProcessParameters actual = parser.run(cmdLine, ports);
		ProcessParameters expected = createStandardCmdLineParams();
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void testTooManyPorts() {
		CmdLineParserForProcessRunParameters parser = 
				new CmdLineParserForProcessRunParameters();
		String[] args = new String[]{
				"-Iperson=/users/joe/person_input",
				"-Idocument=/users/joe/doc_input",
				"-Omerged=/users/joe/merged_out",
				"-Oother=/users/joe/other_out"};
		Ports ports = createStandardPorts();
		CommandLine cmdLine = CmdLineParser.parse(args);
		assertThrows(CmdLineParserException.class, () -> parser.run(cmdLine, ports));
	}
	
	@Test
	public void testTooFewPorts() {
		CmdLineParserForProcessRunParameters parser = 
				new CmdLineParserForProcessRunParameters();
		String[] args = new String[]{
				"-Iperson=/users/joe/person_input",
				"-Omerged=/users/joe/merged_out"};
		Ports ports = createStandardPorts();
		CommandLine cmdLine = CmdLineParser.parse(args);
		assertThrows(CmdLineParserException.class, () -> parser.run(cmdLine, ports));
	}
	
	private static Ports createStandardPorts(){
		HashMap<String, PortType> inputPorts = 
				new HashMap<String, PortType>();
		inputPorts.put("person", new AnyPortType());
		inputPorts.put("document", new AnyPortType());
		HashMap<String, PortType> outputPorts = 
				new HashMap<String, PortType>();
		outputPorts.put("merged", new AnyPortType());
		return new Ports(inputPorts, outputPorts);	
	}
	
	private static ProcessParameters createStandardCmdLineParams() 
			throws URISyntaxException{
		HashMap<String, Path> inputBinding = new HashMap<String, Path>();
		inputBinding.put("person", 
				new Path("/users/joe/person_input"));
		inputBinding.put("document", 
				new Path("/users/joe/doc_input"));
		HashMap<String, Path> outputBinding = new HashMap<String, Path>();
		outputBinding.put("merged", 
				new Path("/users/joe/merged_out"));
		PortBindings expectedBindings = 
				new PortBindings(inputBinding, outputBinding);
		Map<String, String> params = new HashMap<String, String>();
		params.put("age", "33");
		ProcessParameters cmdLineParams = new ProcessParameters(
				expectedBindings, params);
		return cmdLineParams;
	}
}
