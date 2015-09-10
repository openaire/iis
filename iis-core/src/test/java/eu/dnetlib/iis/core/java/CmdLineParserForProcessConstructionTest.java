package eu.dnetlib.iis.core.java;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import eu.dnetlib.iis.core.java.porttype.PortType;

/**
 * 
 * @author Mateusz Kobos
 *
 */
class DummyProcess implements Process {

	public DummyProcess(){	
	}
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return null;
	}
	
	@Override
	public Map<String, PortType> getOutputPorts(){
		return null;
	}

	@Override
	public void run(PortBindings portBindings, Configuration configuration, 
			Map<String, String> parameters){
	}
}

class DummyProcessWithParametersConstructor implements Process{
	public String[] params;
	
	
	public DummyProcessWithParametersConstructor(String[] params){
		this.params = params;
	}

	@Override
	public Map<String, PortType> getInputPorts() {
		return null;
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return null;
	}

	@Override
	public void run(PortBindings portBindings, Configuration configuration,
			Map<String, String> parameters) throws Exception {
	}
	
}

public class CmdLineParserForProcessConstructionTest{

	@Test
	public void testTrivial() {
		String[] args = new String[]{
				"eu.dnetlib.iis.core.java.DummyProcess"};
		CmdLineParserForProcessConstruction cmdLineParser = 
				new CmdLineParserForProcessConstruction();
		CommandLine cmdLine = CmdLineParser.parse(args);
		Process actual = cmdLineParser.run(cmdLine);
		assertEquals(DummyProcess.class, actual.getClass());
	}
	
	@Test
	public void testWithSomeParameters() {
		String[] args = new String[]{
				"eu.dnetlib.iis.core.java.DummyProcess",
				"-Iperson=hdfs://localhost:8020/users/joe/person_input",
				"-Idocument=hdfs://localhost:8020/users/joe/doc_input",
				"-Omerged=hdfs://localhost:8020/users/joe/merged_out",
				"-SclassName=java.util.String"};
		CmdLineParserForProcessConstruction cmdLineParser = 
				new CmdLineParserForProcessConstruction();
		CommandLine cmdLine = CmdLineParser.parse(args);
		Process actual = cmdLineParser.run(cmdLine);
		assertEquals(DummyProcess.class, actual.getClass());
	}
	
	@Test
	public void testWithSomeParametersWithParametersConstructor() {
		String[] constructorParams = new String[]{
				"some string", "some other string"};
		String[] args = new String[]{
				"eu.dnetlib.iis.core.java.DummyProcessWithParametersConstructor",
				"-C" + constructorParams[0],
				"-C" + constructorParams[1],
				"-Iperson=hdfs://localhost:8020/users/joe/person_input",
				"-Idocument=hdfs://localhost:8020/users/joe/doc_input",
				"-Omerged=hdfs://localhost:8020/users/joe/merged_out",
				"-SclassName=java.util.String"};
		CmdLineParserForProcessConstruction cmdLineParser = 
				new CmdLineParserForProcessConstruction();
		CommandLine cmdLine = CmdLineParser.parse(args);
		DummyProcessWithParametersConstructor actual = 
				(DummyProcessWithParametersConstructor) cmdLineParser.run(cmdLine);
		assertEquals(DummyProcessWithParametersConstructor.class, 
				actual.getClass());
		for(int i = 0; i < constructorParams.length; i++){
			assertEquals(constructorParams[i], actual.params[i]);
		}
		
	}
}
