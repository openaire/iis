package eu.dnetlib.iis.common.model.extrainfo.citations;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
        float trustLevel = 0.9f;
        TypedId typedId = new TypedId(value, type, trustLevel);
        
        // execute & assert
        assertNotNull(typedId);
        assertNotEquals("string", typedId);
        assertNotEquals(typedId, new TypedId("otherValue", type, trustLevel));
        assertNotEquals(typedId, new TypedId(value, "otherType", trustLevel));
        assertNotEquals(typedId, new TypedId(value, type, 0.8f));
        assertEquals(typedId, new TypedId(value, type, trustLevel));
    }

    @Test
    public void testHashCode() {
        // given
        String value = "someValue";
        String type = "someType";
        float trustLevel = 0.9f;
        TypedId typedId = new TypedId(value, type, trustLevel);
        
        // execute & assert
        assertNotEquals(typedId.hashCode(), new TypedId("otherValue", type, trustLevel).hashCode());
        assertNotEquals(typedId.hashCode(), new TypedId(value, "otherType", trustLevel).hashCode());
        assertNotEquals(typedId.hashCode(), new TypedId(value, type, 0.8f).hashCode());
        assertEquals(typedId.hashCode(), new TypedId(value, type, trustLevel).hashCode());
    }
}
