package eu.dnetlib.iis.common.pig.udfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author mhorst
 *
 */
public class StringListToListWithIndexesTest {

    private StringListToListWithIndexes udf = new StringListToListWithIndexes();
    
    // --------------------------------- TESTS -------------------------------------
    
    @Test(expected=IOException.class)
    public void testExecOnNull() throws Exception {
        // execute
        udf.exec(null);
    }
    
    @Test(expected=IOException.class)
    public void testExecOnEmpty() throws Exception {
        // given
        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        // execute
        udf.exec(tupleFactory.newTuple());
    }
    
    @Test(expected=IOException.class)
    public void testExecTooManyArgs() throws Exception {
        // given
        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        // execute
        udf.exec(tupleFactory.newTuple(Lists.newArrayList("str1", "str2")));
    }
    
    @Test(expected=ClassCastException.class)
    public void testExecOnString() throws Exception {
        // given
        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        // execute
        udf.exec(tupleFactory.newTuple(Lists.newArrayList("str1")));
    }

    @Test
    public void testExecWithEmptyDataBag() throws Exception {
        // given
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        
        // execute
        DataBag resultBag = udf.exec(tupleFactory.newTuple(bagFactory.newDefaultBag()));
        
        // assert
        assertNotNull(resultBag);
        assertEquals(0, resultBag.size());
    
    }
    @Test
    public void testExec() throws Exception {
        // given
        String tuple1Name = "tuple1";
        String tuple2Name = "tuple2";
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = BagFactory.getInstance();
        DataBag sourceBag = bagFactory.newDefaultBag(Lists.newArrayList(
                tupleFactory.newTuple(tuple1Name), 
                tupleFactory.newTuple(tuple2Name)));
        
        // execute
        DataBag resultBag = udf.exec(tupleFactory.newTuple(sourceBag));
        
        // assert
        assertNotNull(resultBag);
        Iterator<Tuple> tupleIt = resultBag.iterator();
        
        assertTrue(tupleIt.hasNext());
        Tuple firstTuple = tupleIt.next();
        assertEquals(0, firstTuple.get(0));
        assertEquals(tuple1Name, firstTuple.get(1));
        
        assertTrue(tupleIt.hasNext());
        Tuple secondTuple = tupleIt.next();
        assertEquals(1, secondTuple.get(0));
        assertEquals(tuple2Name, secondTuple.get(1));
        
        assertFalse(tupleIt.hasNext());
    }
    
    @Test
    public void testOutputSchema() throws Exception {
        // given
        Schema irrelevantSchema = null;
        
        // execute
        Schema resultSchema = udf.outputSchema(irrelevantSchema);
        
        // assert
        assertNotNull(resultSchema);
        assertEquals(1, resultSchema.getFields().size());
        assertEquals(DataType.BAG, resultSchema.getField(0).type);
        assertEquals(DataType.TUPLE, resultSchema.getField(0).schema.getField(0).type);
        assertEquals(DataType.INTEGER, resultSchema.getField(0).schema.getField(0).schema.getField(0).type);
        assertEquals(DataType.CHARARRAY, resultSchema.getField(0).schema.getField(0).schema.getField(1).type);
    }
    
}
