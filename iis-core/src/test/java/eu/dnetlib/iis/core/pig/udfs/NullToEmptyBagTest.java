package eu.dnetlib.iis.core.pig.udfs;

import com.google.common.collect.Lists;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 *
 * @author Dominika Tkaczyk
 */
public class NullToEmptyBagTest extends TestCase {
    
    @Test
	public void testUDF() throws IOException {
        NullToEmptyBag udf = new NullToEmptyBag();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        DataBag emptyBag = bagFactory.newDefaultBag();
        DataBag bag = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup1"), tupleFactory.newTuple()));
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(tupleFactory.newTuple()));
        assertEquals(emptyBag, udf.exec(tupleFactory.newTuple((DataBag)null)));
        assertEquals(emptyBag, udf.exec(tupleFactory.newTuple(emptyBag)));
        assertEquals(bag, udf.exec(tupleFactory.newTuple(bag)));
        assertNull(udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag, emptyBag))));
    }
    
}
