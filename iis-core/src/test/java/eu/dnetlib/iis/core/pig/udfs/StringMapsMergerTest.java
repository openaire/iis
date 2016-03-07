package eu.dnetlib.iis.core.pig.udfs;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 *
 * @author Dominika Tkaczyk
 */
public class StringMapsMergerTest extends TestCase {
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUDF() throws IOException {
        StringMapsMerger udf = new StringMapsMerger();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Map<String, String> emptyMap = new HashMap<String, String>();
        Map<String, String> map1 = new HashMap<String, String>();
        map1.put("12", "12");
        map1.put("3", "bag");
        map1.put("1", "");
        Map<String, String> map2 = new HashMap<String, String>();
        map2.put("12", "null");
        map2.put("3", "data");
        map2.put("35", "empty");
        Map<String, String> map3 = new HashMap<String, String>();
        map3.put("1", "notempty");
        Map<String, String> map4 = new HashMap<String, String>();
        map4.put("1", "");
        map4.put("3", "bag");
        map4.put("12", "12");
        map4.put("35", "empty");
        
        assertNull(udf.exec(null));
        assertNull(udf.exec(tupleFactory.newTuple()));
        assertNull(udf.exec(tupleFactory.newTuple((Map)null)));
        assertNull(udf.exec(tupleFactory.newTuple(emptyMap)));
        assertEquals(map1, udf.exec(tupleFactory.newTuple(map1)));
        assertEquals(map4, udf.exec(tupleFactory.newTuple(Lists.newArrayList(map1, emptyMap, map2, null, map3))));
    }
    
}
