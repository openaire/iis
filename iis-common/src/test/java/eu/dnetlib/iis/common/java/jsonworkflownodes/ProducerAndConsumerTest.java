package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.util.Map;

import org.junit.Assert;

import org.junit.Test;

import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.utils.AvroUtils;

public class ProducerAndConsumerTest{
	@Test
	public void testPortCreationInProducer(){
		checkPortsCreator(new PortsCreator() {
			@Override 
			public Map<String, PortType> getPorts(String[] specificationStrings){
				Producer producer = 
						new Producer(specificationStrings);
				Assert.assertEquals(0, producer.getInputPorts().size());
				return producer.getOutputPorts();
			}
		});
	}
	
	@Test
	public void testPortCreationInTestingConsumer(){
		checkPortsCreator(new PortsCreator() {
			@Override 
			public Map<String, PortType> getPorts(String[] specificationStrings){
				TestingConsumer consumer = 
						new TestingConsumer(specificationStrings);
				Assert.assertEquals(0, consumer.getOutputPorts().size());
				return consumer.getInputPorts();
			}
		});
	}
	
	private void checkPortsCreator(PortsCreator creator) {
		PortSpec[] specs = new PortSpec[] {
				new PortSpec(
						"document",
						"eu.dnetlib.iis.common.avro.Document",
						"fake/path/document.json"),
				new PortSpec("person",
						"eu.dnetlib.iis.common.avro.Person",
						"fake/path/person.json") };
		String[] specsStr = new String[specs.length];
		for (int i = 0; i < specs.length; i++) {
			specsStr[i] = String.format("{%s, %s, %s}", specs[i].name,
					specs[i].schemaPath, specs[i].jsonPath);
		}
		Map<String, PortType> outs = creator.getPorts(specsStr);
		Assert.assertEquals(specs.length, outs.size());
		for (int i = 0; i < specs.length; i++) {
			AvroPortType actual = (AvroPortType) outs.get(specs[i].name);
			AvroPortType expected = new AvroPortType(
					AvroUtils.toSchema(specs[i].schemaPath));
			Assert.assertEquals(expected, actual);
		}
	}
}

interface PortsCreator{
	Map<String, PortType> getPorts(String[] specificationStrings);
}

class PortSpec{
	public String name;
	public String schemaPath;
	public String jsonPath;
	
	public PortSpec(String name, String schemaPath, String jsonPath){
		this.name = name;
		this.schemaPath = schemaPath;
		this.jsonPath = jsonPath;
	}
}