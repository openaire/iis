package eu.dnetlib.iis.common.pig.udfs;

import com.google.common.collect.Lists;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link IdConfidenceTupleDeduplicator} UDF test class.
 * @author mhorst
 *
 */
public class IdConfidenceTupleDeduplicatorTest {
	
	@Test
	public void testUDF() throws IOException {
	    // given
		IdConfidenceTupleDeduplicator udf = new IdConfidenceTupleDeduplicator();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        DataBag emptyBag = bagFactory.newDefaultBag();
        
        
        // execute & assert
        assertNull(udf.exec(null));
        assertNull(udf.exec(tupleFactory.newTuple()));
        assertNull(udf.exec(tupleFactory.newTuple((DataBag)null)));
        assertEquals(emptyBag, udf.exec(tupleFactory.newTuple(emptyBag)));
        
        assertEquals(
//        		expected
        		bagFactory.newDefaultBag(Collections.singletonList(
        				tupleFactory.newTuple(Arrays.asList("tuple1", null)))), 
//              provided
                udf.exec(tupleFactory.newTuple(
                		bagFactory.newDefaultBag(Collections.singletonList(
                				tupleFactory.newTuple(Arrays.asList("tuple1", null)))))));
        
        assertEquals(
//        		expected
        		bagFactory.newDefaultBag(Collections.singletonList(
        				tupleFactory.newTuple(Arrays.asList("tuple1",0.9f)))), 
//              provided
                udf.exec(tupleFactory.newTuple(
                		bagFactory.newDefaultBag(Lists.newArrayList(
                				tupleFactory.newTuple(Arrays.asList("tuple1",0.9f)),
                				tupleFactory.newTuple(Arrays.asList("tuple1",0.1f)),
                				tupleFactory.newTuple(Arrays.asList("tuple1",null)))))));
        
        assertEquals(
//        		expected
        		bagFactory.newDefaultBag(Lists.newArrayList(
        				tupleFactory.newTuple(Arrays.asList("tuple1",null)),
        				tupleFactory.newTuple(Arrays.asList("tuple2",0.1f)),
        				tupleFactory.newTuple(Arrays.asList("tuple3",0.6f)))), 
//              provided
                udf.exec(tupleFactory.newTuple(
                		bagFactory.newDefaultBag(Lists.newArrayList(
                				tupleFactory.newTuple(Arrays.asList("tuple3",0.3f)),
                				tupleFactory.newTuple(Arrays.asList("tuple2",0.1f)),
                				tupleFactory.newTuple(Arrays.asList("tuple3",0.6f)),
                				tupleFactory.newTuple(Arrays.asList("tuple1",null)))))));
    }
	
	@Test
    public void testOutputSchema() {
        // given
	    IdConfidenceTupleDeduplicator udf = new IdConfidenceTupleDeduplicator();
	    Schema inputSchema = new Schema();
	    inputSchema.add(new FieldSchema(null, DataType.CHARARRAY));
        
        // execute
        Schema resultSchema = udf.outputSchema(inputSchema);
        
        // assert
		assertSame(inputSchema, resultSchema);
    }
}
