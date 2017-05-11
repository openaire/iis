package eu.dnetlib.iis.common.pig.udfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

/**
 * @author mhorst
 *
 */
public class EmptyMapTest {

    private EmptyMap udf = new EmptyMap();
    
    // --------------------------------- TESTS -------------------------------------
    
    @Test
    public void testExec() throws Exception {
        // execute
        Map<String, String> result = udf.exec(null);
        
        // assert
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testOutputSchema() throws Exception {
        // given
        Schema irrelevantSchema = null;
        
        // execute
        Schema resultSchema = udf.outputSchema(irrelevantSchema);
        
        // assert
        assertNotNull(resultSchema);
        assertEquals(1, resultSchema.getFields().size());
        assertEquals(DataType.MAP, resultSchema.getField(0).type);
    }

}
