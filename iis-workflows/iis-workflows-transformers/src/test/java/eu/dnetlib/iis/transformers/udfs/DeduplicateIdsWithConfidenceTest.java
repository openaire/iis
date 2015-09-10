package eu.dnetlib.iis.transformers.udfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * {@link DeduplicateIdsWithConfidence} UDF test class.
 * @author mhorst
 *
 */
public class DeduplicateIdsWithConfidenceTest {
	
	@SuppressWarnings("unchecked")
	@Test
	public void testUDF() throws IOException {
		DeduplicateIdsWithConfidence udf = new DeduplicateIdsWithConfidence();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        DataBag emptyBag = bagFactory.newDefaultBag();
        
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
}
