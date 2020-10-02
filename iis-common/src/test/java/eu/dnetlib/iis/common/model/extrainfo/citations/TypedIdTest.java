package eu.dnetlib.iis.common.model.extrainfo.citations;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author mhorst
 *
 */
public class TypedIdTest {

    @Test
    public void testEquals() {
        // given
        String value = "someValue";
        String type = "someType";
        float confidenceLevel = 0.9f;
        TypedId typedId = new TypedId(value, type, confidenceLevel);
        
        // execute & assert
        assertNotEquals(null, typedId);
        assertNotEquals("string", typedId);
        assertNotEquals(typedId, new TypedId("otherValue", type, confidenceLevel));
        assertNotEquals(typedId, new TypedId(value, "otherType", confidenceLevel));
        assertNotEquals(typedId, new TypedId(value, type, 0.8f));
        assertEquals(typedId, new TypedId(value, type, confidenceLevel));
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
