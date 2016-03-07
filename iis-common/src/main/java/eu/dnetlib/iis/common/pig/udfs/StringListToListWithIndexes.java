package eu.dnetlib.iis.common.pig.udfs;

import com.google.common.collect.Lists;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

/**
 * 
 * @author Dominika Tkaczyk
 */
public class StringListToListWithIndexes extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() != 1) {
            throw new IOException("UDF StringListToListWithIndexes takes one argument: a list of strings");
        }
        
        DataBag stringBag = (DataBag) tuple.get(0);
        
        BagFactory bagFactory = BagFactory.getInstance();
        DataBag indexedBag = bagFactory.newDefaultBag();
        
        if (stringBag == null) {
            return indexedBag;
        }

        TupleFactory tupleFactory = TupleFactory.getInstance();
        
        int i = 0;
        for (Tuple stringTuple : stringBag) {
            Tuple indexedTuple = tupleFactory.newTuple(Lists.newArrayList(i++, stringTuple.get(0)));
            indexedBag.add(indexedTuple);
        }
        
        return indexedBag;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return SchemaUtil.newBagSchema(Lists.newArrayList(DataType.INTEGER, DataType.CHARARRAY));
        } catch (FrontendException ex) {
            return null;
        }
    }
    
}
