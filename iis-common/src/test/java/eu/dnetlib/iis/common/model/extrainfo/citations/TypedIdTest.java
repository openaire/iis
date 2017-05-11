package eu.dnetlib.iis.common.model.extrainfo.citations;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class TypedIdTest {

    @Test
    public void testEquals() throws Exception {
        // given
        String value = "someValue";
        String type = "someType";
        float confidenceLevel = 0.9f;
        TypedId typedId = new TypedId(value, type, confidenceLevel);
        
        // execute & assert
        assertFalse(typedId.equals(null));
        assertFalse(typedId.equals("string"));
        assertFalse(typedId.equals(new TypedId("otherValue", type, confidenceLevel)));
        assertFalse(typedId.equals(new TypedId(value, "otherType", confidenceLevel)));
        assertFalse(typedId.equals(new TypedId(value, type, 0.8f)));
        assertTrue(typedId.equals(new TypedId(value, type, confidenceLevel)));
    }

    @Test
    public void testHashCode() throws Exception {
        // given
        String value = "someValue";
        String type = "someType";
        float confidenceLevel = 0.9f;
        TypedId typedId = new TypedId(value, type, confidenceLevel);
        
        // execute & assert
        assertNotEquals(typedId.hashCode(), new TypedId("otherValue", type, confidenceLevel).hashCode());
        assertNotEquals(typedId.hashCode(), new TypedId(value, "otherType", confidenceLevel).hashCode());
        assertNotEquals(typedId.hashCode(), new TypedId(value, type, 0.8f).hashCode());
        assertEquals(typedId.hashCode(), new TypedId(value, type, confidenceLevel).hashCode());
    }
    
}
