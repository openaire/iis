package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.io.IOUtils;
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
		accessor.registerDecoder("targetValue", new OafFieldDecoder());

		String rawSetId = "rawset-id";
		String agentId = "agent-id";
		String agentName = "agent-name";
		AtomicAction action = new AtomicAction(rawSetId, new Agent(agentId, agentName, Agent.AGENT_TYPE.service));
		byte[] decodedTargetValue = decodeTargetValue(new String(
				IOUtils.toByteArray(FieldAccessorTest.class
				.getResourceAsStream("/eu/dnetlib/iis/wf/export/actionmanager/sequencefile/resultProject.json")),
				"utf8"));
		action.setTargetValue(decodedTargetValue);

		assertEquals(rawSetId, accessor.getValue("rawSet", action));
		assertEquals(agentId, accessor.getValue("agent.id", action));
		assertEquals(agentName, accessor.getValue("agent.name", action));
		assertEquals(Agent.AGENT_TYPE.service, accessor.getValue("agent.type", action));
		assertEquals("relation", accessor.getValue("$targetValue.kind", action).toString());
		assertEquals("resultProject", accessor.getValue("$targetValue.rel.relType", action).toString());
	}

	@Test(expected=Exception.class)
	public void testAccessingValuesForInvalidPath() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("targetValue", new OafFieldDecoder());

		String rawSetId = "rawset-id";
		String agentId = "agent-id";
		String agentName = "agent-name";
		AtomicAction action = new AtomicAction(rawSetId, new Agent(agentId, agentName, 
				Agent.AGENT_TYPE.service));

		accessor.getValue("agent.unknown", action);
	}
	
	@Test
	public void testAccessingNullValue() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("targetValue", new OafFieldDecoder());
		AtomicAction action = new AtomicAction(null, new Agent("agent-id", "agent-name", 
				Agent.AGENT_TYPE.service));
		assertNull(accessor.getValue("rawSet", action));
	}
	
	@Test
	public void testAccessingArrayElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1","name-2"});
		assertEquals("name-1", accessor.getValue("names[0]", object));
		assertEquals("name-2", accessor.getValue("names[1]", object));
	}
	
	@Test(expected=Exception.class)
	public void testAccessingNonExistingArrayElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1","name-2"});
		accessor.getValue("names[2]", object);
	}
	
	@Test
	public void testAccessingArrayElementWithInvalidPath() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1", "name-2"});
		try {
			assertEquals("name-1", accessor.getValue("names[[0]", object));
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("names[", object));
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("names]", object));
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(Exception e) {
//			OK
		}
//		we should be able to handle this scenario
		assertEquals("name-1", accessor.getValue("names[0]]", object));
	}
	
	@Test
	public void testAccessingListElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ListWrapper object = new ListWrapper(Arrays.asList(new String[] {"name-1", "name-2"}));
		assertEquals("name-1", accessor.getValue("names[0]", object));
		assertEquals("name-2", accessor.getValue("names[1]", object));
	}
	
	@Test(expected=Exception.class)
	public void testAccessingNonExistingListElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ListWrapper object = new ListWrapper(Arrays.asList(new String[] {"name-1", "name-2"}));
		accessor.getValue("names[2]", object);
	}
	
	@Test
	public void testAccessingDecodedArrayElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("names", new ArrayProducingDecoder(new String[] {"decoded-1", "decoded-2"}));
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1","name-2"});
		assertEquals("decoded-1", accessor.getValue("$names[0]", object));
		assertEquals("decoded-2", accessor.getValue("$names[1]", object));
	}
	
	@Test
	public void testAccessingValuesWithPropertyUtils() throws Exception {
		String[] array = new String[] {"name-1","name-2"};
		ArrayWrapper object = new ArrayWrapper(array);
		assertEquals(array, PropertyUtils.getProperty(object, "names"));
		assertEquals("name-1", PropertyUtils.getIndexedProperty(object, "names", 0));
	}
	
	public static class ArrayWrapper {
		final String[] _names;
		public ArrayWrapper(String[] names) {
			this._names = names;
		}
		public String[] getNames() {
			return _names;
		}
	}
	
	public static class ListWrapper {
		private List<String> _names;
		public void setNames(List<String> names) {
			this._names = names;
		}
		public ListWrapper(List<String> names) {
			this._names = names;
		}
		public List<String> getNames() {
			return _names;
		}
	}
	
	public static class ArrayProducingDecoder implements FieldDecoder {

		String[] predefinedArray;
		
		public ArrayProducingDecoder(String[] predefinedArray) {
			this.predefinedArray = predefinedArray;
		}
		
		@Override
		public Object decode(Object source) throws FieldDecoderException {
			return predefinedArray;
		}

		@Override
		public boolean canHandle(Object source) {
			return true;
		}
		
	}
	
	private byte[] decodeTargetValue(final String json) throws Exception {
		OafProtos.Oaf.Builder oaf = OafProtos.Oaf.newBuilder();
		JsonFormat.merge(json, oaf);
		return oaf.build().toByteArray();
	}
}
