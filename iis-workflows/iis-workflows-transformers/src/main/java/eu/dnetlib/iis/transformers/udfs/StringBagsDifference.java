package eu.dnetlib.iis.transformers.udfs;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

/**
 * Computes the difference between two data bags.
 * The output data bag contains those elements from the first input bag, that 
 * were not present in the second one.
 *
 * @author Dominika Tkaczyk
 */
public class StringBagsDifference extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() != 2) {
            return null;
        }
        DataBag dbMain = (DataBag) tuple.get(0);
        DataBag dbSub = (DataBag) tuple.get(1);
        if (dbMain == null || dbSub == null) {
            return dbMain;
        }
        
        List<Tuple> tuples = new ArrayList<Tuple>();
        Iterator<Tuple> itMain = dbMain.iterator();
        while (itMain.hasNext()) {
            tuples.add(itMain.next());
        }
        Iterator<Tuple> itSub = dbSub.iterator();
        while (itSub.hasNext()) {
            tuples.remove(itSub.next());
        }
        if (tuples.isEmpty()) {
            return null;
        }

        BagFactory bagFactory = BagFactory.getInstance();
        return bagFactory.newDefaultBag(tuples);
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return SchemaUtil.newBagSchema(Lists.newArrayList(DataType.CHARARRAY));
        } catch (FrontendException ex) {
            return null;
        }
    }
    
}
