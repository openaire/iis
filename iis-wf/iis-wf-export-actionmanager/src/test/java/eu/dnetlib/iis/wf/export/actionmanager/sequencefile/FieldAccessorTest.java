package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.cxf.helpers.IOUtils;
import org.junit.Test;

import com.googlecode.protobuf.format.JsonFormat;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.OafProtos;

/**
 * Field accessor test class.
 * @author mhorst
 *
 */
public class FieldAccessorTest {

	@Test
	public void testAccessingValuesUsingDecoder() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("TargetValue", new OafFieldDecoder());

		String rawSetId = "rawset-id";
		String agentId = "agent-id";
		String agentName = "agent-name";
		AtomicAction action = new AtomicAction(rawSetId, new Agent(agentId, agentName, Agent.AGENT_TYPE.service));
		byte[] decodedTargetValue = decodeTargetValue(new String(IOUtils.readBytesFromStream(FieldAccessorTest.class
				.getResourceAsStream("/eu/dnetlib/iis/wf/export/actionmanager/sequencefile/resultProject.json")),
				"utf8"));
		action.setTargetValue(decodedTargetValue);

		assertEquals(rawSetId, accessor.getValue("RawSet", action).toString());
		assertEquals(agentId, accessor.getValue("Agent.Id", action).toString());
		assertEquals(agentName, accessor.getValue("Agent.Name", action).toString());
		assertEquals(Agent.AGENT_TYPE.service.toString(), accessor.getValue("Agent.Type", action).toString());
		assertEquals("relation", accessor.getValue("$TargetValue.Kind", action).toString());
		assertEquals("resultProject", accessor.getValue("$TargetValue.Rel.RelType", action).toString());
	}

	@Test(expected=Exception.class)
	public void testAccessingValuesForInvalidPath() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("TargetValue", new OafFieldDecoder());

		String rawSetId = "rawset-id";
		String agentId = "agent-id";
		String agentName = "agent-name";
		AtomicAction action = new AtomicAction(rawSetId, new Agent(agentId, agentName, 
				Agent.AGENT_TYPE.service));

		accessor.getValue("$TargetValue.Unknown", action).toString();
	}
	
	@Test
	public void testAccessingNullValue() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("TargetValue", new OafFieldDecoder());
		AtomicAction action = new AtomicAction(null, new Agent("agent-id", "agent-name", 
				Agent.AGENT_TYPE.service));
		assertNull(accessor.getValue("RawSet", action));
	}
	
	@Test(expected=Exception.class)
	public void testAccessingArrayElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1","name-2"});
		assertEquals("name-1", accessor.getValue("Names[0]", object).toString());
		assertEquals("name-2", accessor.getValue("Names[1]", object).toString());
		accessor.getValue("Names[2]", object);
		
	}
	
	public void testAccessingArrayElementWithInvalidPath() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1", "name-2"});
		try {
			assertEquals("name-1", accessor.getValue("Names[[0]", object).toString());
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("Names[", object).toString());
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("Names]", object).toString());
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("Names[0]]", object).toString());
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
		
	}
	
	@Test(expected=Exception.class)
	public void testAccessingListElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ListWrapper object = new ListWrapper(Arrays.asList(new String[] {"name-1", "name-2"}));
		assertEquals("name-1", accessor.getValue("Names[0]", object).toString());
		assertEquals("name-2", accessor.getValue("Names[1]", object).toString());
		accessor.getValue("Names[2]", object);
	}
	
	class ArrayWrapper {
		final String[] names;
		public ArrayWrapper(String[] names) {
			this.names = names;
		}
		public String[] getNames() {
			return names;
		}
	}
	
	class ListWrapper {
		final List<String> names;
		public ListWrapper(List<String> names) {
			this.names = names;
		}
		public List<String> getNames() {
			return names;
		}
	}
	
	private byte[] decodeTargetValue(final String json) throws Exception {
		OafProtos.Oaf.Builder oaf = OafProtos.Oaf.newBuilder();
		JsonFormat.merge(json, oaf);
		return oaf.build().toByteArray();
	}
}
