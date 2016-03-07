package eu.dnetlib.iis.common.pig.udfs;

import com.google.common.collect.Lists;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 *
 * @author Dominika Tkaczyk
 */
public class IntegerFirstNotEmptyTest extends TestCase {
    
    @Test
	public void testUDF() throws IOException {
        IntegerFirstNotEmpty udf = new IntegerFirstNotEmpty();
        TupleFactory factory = TupleFactory.getInstance();
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(factory.newTuple()));
        assertNull(udf.exec(factory.newTuple((Integer) null)));
        assertEquals(125, (int)udf.exec(factory.newTuple(Integer.valueOf(125))));
        assertNull(udf.exec(factory.newTuple(Lists.newArrayList())));
        assertNull(udf.exec(factory.newTuple(Lists.newArrayList(null, null))));
        assertEquals(23, (int)udf.exec(factory.newTuple(Lists.newArrayList(23, null, 256, 90))));
        assertEquals(256, (int)udf.exec(factory.newTuple(Lists.newArrayList(null, null, null, 256, 567))));
    }
    
}
