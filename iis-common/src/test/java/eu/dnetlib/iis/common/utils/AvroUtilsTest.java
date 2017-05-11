package eu.dnetlib.iis.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.avro.Schema;
import org.junit.Test;

import eu.dnetlib.iis.common.schemas.Identifier;

/**
 * @author mhorst
 *
 */
public class AvroUtilsTest {

    // ---------------------------------------------- TESTS ------------------------------------------------
    
    @Test(expected=IllegalArgumentException.class)
    public void testToSchemaForNotExistingPrimitive() throws Exception {
        // execute
        AvroUtils.toSchema("org.apache.avro.Schema.Type.NON_EXISTING");
    }
    
    @Test
    public void testToSchemaForPrimitive() throws Exception {
        // execute
        Schema schema = AvroUtils.toSchema("org.apache.avro.Schema.Type.STRING");
        
        // assert
        assertNotNull(schema);
        assertTrue(schema.equals(Schema.create(Schema.Type.STRING)));
    }
    
    @Test(expected=RuntimeException.class)
    public void testToSchemaForNonExistingAvroClass() throws Exception {
        // execute
        AvroUtils.toSchema("non.existing.Class");
    }
    
    @Test
    public void testToSchemaForAvroClass() throws Exception {
        // execute
        Schema schema = AvroUtils.toSchema(Identifier.class.getCanonicalName());
        
        // assert
        assertNotNull(schema);
        assertTrue(schema == Identifier.SCHEMA$);
    }
    
    @Test(expected=RuntimeException.class)
    public void testGetCopyForNonIndexedRecord() throws Exception {
        AvroUtils.getCopy("source", Identifier.SCHEMA$, String.class);
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
