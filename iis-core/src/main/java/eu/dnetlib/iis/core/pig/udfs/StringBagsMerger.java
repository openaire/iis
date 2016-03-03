package eu.dnetlib.iis.core.pig.udfs;

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
 * Merges two or more data bags containing chararray elements.
 * The output data bag contains the elements from all input bags without repetitions.
 *
 * @author Dominika Tkaczyk
 */
public class StringBagsMerger extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        List<Tuple> tuples = new ArrayList<Tuple>();
        for (int i = 0; i < tuple.size(); i++) {
            if (tuple.get(i) != null) {
                DataBag db = (DataBag) tuple.get(i);
                Iterator<Tuple> it = db.iterator();
                while (it.hasNext()) {
                    Tuple next = it.next();
                    if (!tuples.contains(next)) {
                        tuples.add(next);
                    }
                }
            }
        }
        if (tuples.isEmpty()) {
            return null;
        }

        BagFactory bagFactory = BagFactory.getInstance();
        return bagFactory.newDefaultBag(Lists.newArrayList(tuples));
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
