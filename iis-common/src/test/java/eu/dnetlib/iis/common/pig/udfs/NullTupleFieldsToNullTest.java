package eu.dnetlib.iis.common.pig.udfs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Test;

import com.google.common.collect.Lists;

import junit.framework.TestCase;

/**
 *
 * @author Dominika Tkaczyk
 */
public class NullTupleFieldsToNullTest extends TestCase {
    
    @Test
	public void testUDF() throws IOException {
        NullTupleFieldsToNull udf = new NullTupleFieldsToNull();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        
        DataBag emptyBag = bagFactory.newDefaultBag();
        DataBag bag = bagFactory.newDefaultBag(Arrays.asList(new Tuple[]{tupleFactory.newTuple("tup1")}));
        Tuple nullTuple = tupleFactory.newTuple(Lists.newArrayList(null, null, null));
        Tuple tuple = tupleFactory.newTuple(Lists.newArrayList(null, null, "tup1"));
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(tupleFactory.newTuple()));
        assertNull(udf.exec(tupleFactory.newTuple((Tuple)null)));
        assertNull(udf.exec(tupleFactory.newTuple(tupleFactory.newTuple())));
        assertNull(udf.exec(tupleFactory.newTuple(nullTuple)));
        assertEquals(tuple, udf.exec(tupleFactory.newTuple(tuple)));
        assertNull(udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag, emptyBag))));
    }
    
    @Test
    public void testOutputSchema() throws Exception {
        // given
        NullTupleFieldsToNull udf = new NullTupleFieldsToNull();
        Schema inputSchema = new Schema();
        inputSchema.add(new FieldSchema(null, DataType.CHARARRAY));
        
        // execute
        Schema resultSchema = udf.outputSchema(inputSchema);
        
        // assert
        assertTrue(inputSchema == resultSchema);
    }
    
}
