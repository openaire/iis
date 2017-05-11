package eu.dnetlib.iis.common.pig.udfs;

import java.io.IOException;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

import com.google.common.collect.Lists;

import junit.framework.TestCase;

/**
 *
 * @author Dominika Tkaczyk
 */
public class EmptyBagToNullTest extends TestCase {
    
    @Test
	public void testUDF() throws IOException {
        EmptyBagToNull udf = new EmptyBagToNull();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        
        DataBag emptyBag = bagFactory.newDefaultBag();
        DataBag bag = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup1"), tupleFactory.newTuple()));
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(tupleFactory.newTuple()));
        assertNull(udf.exec(tupleFactory.newTuple((DataBag)null)));
        assertNull(udf.exec(tupleFactory.newTuple(emptyBag)));
        assertEquals(bag, udf.exec(tupleFactory.newTuple(bag)));
        assertNull(udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag, emptyBag))));
    }
    
    @Test
    public void testOutputSchema() throws Exception {
        // given
        EmptyBagToNull udf = new EmptyBagToNull();
        Schema irrelevantSchema = null;
        
        // execute
        Schema resultSchema = udf.outputSchema(irrelevantSchema);
        
        // assert
        assertNotNull(resultSchema);
        assertEquals(1, resultSchema.getFields().size());
        assertEquals(DataType.BAG, resultSchema.getField(0).type);
        assertEquals(1, resultSchema.getField(0).schema.getFields().size());
        assertEquals(DataType.TUPLE, resultSchema.getField(0).schema.getField(0).type);
        assertEquals(1, resultSchema.getField(0).schema.getField(0).schema.getFields().size());
        assertEquals(DataType.CHARARRAY, resultSchema.getField(0).schema.getField(0).schema.getField(0).type);
    }
    
}
