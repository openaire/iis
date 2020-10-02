package eu.dnetlib.iis.common.pig.udfs;

import com.google.common.collect.Lists;
import org.apache.pig.data.TupleFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 *
 * @author Dominika Tkaczyk
 */
public class StringFirstNotEmptyTest {
    
    @Test
	public void testUDF() throws IOException {
        StringFirstNotEmpty udf = new StringFirstNotEmpty();
        TupleFactory factory = TupleFactory.getInstance();
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(factory.newTuple()));
        assertNull(udf.exec(factory.newTuple((String) null)));
        assertEquals("tup", udf.exec(factory.newTuple("tup")));
        assertNull(udf.exec(factory.newTuple(Lists.newArrayList())));
        assertNull(udf.exec(factory.newTuple(Lists.newArrayList(null, null))));
        assertEquals("val1", udf.exec(factory.newTuple(Lists.newArrayList("val1", null, "256", "90"))));
        assertEquals("k256", udf.exec(factory.newTuple(Lists.newArrayList(null, null, null, "k256", "567"))));
        assertEquals("k256", udf.exec(factory.newTuple(Lists.newArrayList(null, "", null, "k256", "567"))));
    }
    
}
