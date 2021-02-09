package eu.dnetlib.iis.common.model.extrainfo.citations;

import com.thoughtworks.xstream.XStream;
import org.junit.jupiter.api.DisplayName;
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

    @Test
    @DisplayName("Serialization to XML works correctly")
    public void givenTypedIdInstance_whenSerializedToXML_thenProperResultIsReturned() {
        TypedId typedId = new TypedId("someValue", "someType", 0.5f);
        String xml = "<eu.dnetlib.iis.common.model.extrainfo.citations.TypedId><value>someValue</value><type>someType</type><trustLevel>0.5</trustLevel></eu.dnetlib.iis.common.model.extrainfo.citations.TypedId>";
        XStream xStream = new XStream();
        assertEquals(xStream.fromXML(xml), typedId);
    }
}
