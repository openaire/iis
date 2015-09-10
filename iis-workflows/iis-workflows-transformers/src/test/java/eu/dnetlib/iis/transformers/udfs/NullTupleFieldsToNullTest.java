package eu.dnetlib.iis.transformers.udfs;

import com.google.common.collect.Lists;
import java.io.IOException;
import junit.framework.TestCase;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

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
        DataBag bag = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup1"), null));
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
    
}
