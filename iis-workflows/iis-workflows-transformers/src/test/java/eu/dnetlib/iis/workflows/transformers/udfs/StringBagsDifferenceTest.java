package eu.dnetlib.iis.workflows.transformers.udfs;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.workflows.transformers.udfs.StringBagsDifference;

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
public class StringBagsDifferenceTest extends TestCase {
    
    @Test
	public void testUDF() throws IOException {
        StringBagsDifference udf = new StringBagsDifference();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        DataBag nullBag = null;
        DataBag emptyBag = bagFactory.newDefaultBag();
        DataBag bag1 = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup1"), 
                                                                   tupleFactory.newTuple("tup2")));
        DataBag bag2 = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup3"), 
                                                                   tupleFactory.newTuple("tup4")));
        DataBag bag3 = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup1"), 
                                                                   tupleFactory.newTuple("tup4"),
                                                                   tupleFactory.newTuple("tup5")));
        DataBag bag4 = bagFactory.newDefaultBag(Lists.newArrayList(tupleFactory.newTuple("tup1"), 
                                                                   tupleFactory.newTuple("tup5")));
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(tupleFactory.newTuple()));
        assertNull(udf.exec(tupleFactory.newTuple(nullBag)));
        assertNull(udf.exec(tupleFactory.newTuple(emptyBag)));
        assertNull(udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag1, emptyBag, bag2, bag3))));
        assertNull(udf.exec(tupleFactory.newTuple(Lists.newArrayList(nullBag, bag3, bag2))));
        assertEquals(bag1, udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag1, bag2))));
        assertEquals(bag4, udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag3, bag2))));
        assertNull(udf.exec(tupleFactory.newTuple(Lists.newArrayList(bag4, bag3))));
    }
    
}
