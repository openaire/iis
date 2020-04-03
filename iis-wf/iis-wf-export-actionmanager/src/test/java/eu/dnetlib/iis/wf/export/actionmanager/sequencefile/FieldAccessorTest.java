package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtils;
import org.junit.Test;

import datafu.com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Relation;



/**
 * Field accessor test class.
 * @author mhorst
 *
 */
public class FieldAccessorTest {

	//------------------------ TESTS ---------------------------------
	
	@Test
	public void testAccessingValuesUsingDecoder() throws Exception {
	    String targetId = "addedTarget";
	    FieldAccessor accessor = new FieldAccessor();
		accessor.registerDecoder("payload", new FieldDecoder() {
            @Override
            public Object decode(Object source) throws FieldDecoderException {
                Relation rel = (Relation) source;
                rel.setTarget(targetId);
                return source;
            }
            @Override
            public boolean canHandle(Object source) {
                return source instanceof Relation;
            }
        });
		AtomicAction<Relation> action = new AtomicAction<>();
		action.setClazz(Relation.class);
		
		Relation relation = new Relation();
		action.setPayload(relation);
		
		String sourceId = "someSource";
		relation.setSource(sourceId);
		
		assertEquals(Relation.class.getCanonicalName(), accessor.getValue("clazz.canonicalName", action).toString());
		assertEquals(sourceId, accessor.getValue("$payload.source", action).toString());
		assertEquals(targetId, accessor.getValue("$payload.target", action).toString());
	}
	
	@Test(expected=FieldAccessorException.class)
    public void testAccessingValuesUsingNonExistingDecoder() throws Exception {
        FieldAccessor accessor = new FieldAccessor();

        AtomicAction<Relation> action = new AtomicAction<>();
        Relation relation = new Relation();
        action.setPayload(relation);
        String sourceId = "someSource";
        relation.setSource(sourceId);
        
        assertEquals(sourceId, accessor.getValue("$payload.source", action).toString());
    }
	
	@Test
    public void testAccessingValuesFromActionModel() throws Exception {
        FieldAccessor accessor = new FieldAccessor();

        AtomicAction<Relation> action = new AtomicAction<>();
        action.setClazz(Relation.class);
        
        Relation relation = new Relation();
        action.setPayload(relation);
        
        String sourceId = "someSource";
        relation.setSource(sourceId);
        List<KeyValue> collectedFrom = Lists.newArrayList();
        String key = "someKey";
        KeyValue keyValue = new KeyValue();
        keyValue.setKey(key);
        collectedFrom.add(keyValue);
        relation.setCollectedFrom(collectedFrom);
        
        assertEquals(sourceId, accessor.getValue("payload.source", action).toString());
        assertNotNull(accessor.getValue("payload.collectedFrom", action));
        assertEquals(key, accessor.getValue("payload.collectedFrom[0].key", action).toString());
        assertEquals(Relation.class.getName(), accessor.getValue("clazz.name", action).toString());
        
    }

	@Test(expected=FieldAccessorException.class)
	public void testAccessingValuesForInvalidPath() throws Exception {
		FieldAccessor accessor = new FieldAccessor();

		AtomicAction<Relation> action = new AtomicAction<>();
		action.setClazz(Relation.class);

		accessor.getValue("clazz.unknown", action);
	}
	
	@Test
	public void testAccessingNullValue() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		AtomicAction<Relation> action = new AtomicAction<>();
        
		assertNull(accessor.getValue("payload", action));
	}
	
	@Test
	public void testAccessingArrayElement() throws Exception {
		FieldAccessor accessor = new FieldAccessor();
		ArrayWrapper object = new ArrayWrapper(new String[] {"name-1","name-2"});
		assertEquals("name-1", accessor.getValue("names[0]", object));
		assertEquals("name-2", accessor.getValue("names[1]", object));
	}
	
	@Test(expected=FieldAccessorException.class)
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
		} catch(FieldAccessorException e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("names[", object));
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(FieldAccessorException e) {
//			OK
		}
		try {
			assertEquals("name-1", accessor.getValue("names]", object));
			fail("Exception should be thrown when accessing array with invalid field path");
		} catch(FieldAccessorException e) {
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
	
	@Test(expected=FieldAccessorException.class)
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

	//------------------------ INNER CLASSES  ------------------------
	
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
}
