package eu.dnetlib.iis.common.utils;

import eu.dnetlib.iis.common.schemas.Identifier;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class AvroUtilsTest {

    // ---------------------------------------------- TESTS ------------------------------------------------
    
    @Test
    public void testToSchemaForNotExistingPrimitive() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                AvroUtils.toSchema("org.apache.avro.Schema.Type.NON_EXISTING"));
    }
    
    @Test
    public void testToSchemaForPrimitive() {
        // execute
        Schema schema = AvroUtils.toSchema("org.apache.avro.Schema.Type.STRING");
        
        // assert
        assertNotNull(schema);
        assertEquals(schema, Schema.create(Schema.Type.STRING));
    }

    @Test
    public void testToSchemaForNonExistingAvroClass() {
        // execute
        assertThrows(RuntimeException.class, () -> AvroUtils.toSchema("non.existing.Class"));
    }
    
    @Test
    public void testToSchemaForAvroClass() {
        // execute
        Schema schema = AvroUtils.toSchema(Identifier.class.getCanonicalName());
        
        // assert
        assertNotNull(schema);
        assertSame(schema, Identifier.SCHEMA$);
    }
    
    @Test
    public void testGetCopyForNonIndexedRecord() {
        assertThrows(RuntimeException.class, () -> AvroUtils.getCopy("source", Identifier.SCHEMA$, String.class));
    }
    
    @Test
    public void testGetCopy() throws Exception {
        // given
        String id = "id1";
        Identifier.Builder idBuilder = Identifier.newBuilder();
        idBuilder.setId(id);
        
        // execute
        Identifier result = AvroUtils.getCopy(idBuilder.build(), 
                Identifier.SCHEMA$, Identifier.class);
        
        // assert
        assertNotNull(result);
        assertEquals(id, result.getId().toString());
    }

}
