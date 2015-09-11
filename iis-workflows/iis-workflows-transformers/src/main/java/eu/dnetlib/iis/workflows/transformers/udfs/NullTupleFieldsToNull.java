package eu.dnetlib.iis.workflows.transformers.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Converts null value to an empty DataBag.
 * 
 * @author Dominika Tkaczyk
 */
public class NullTupleFieldsToNull extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() != 1) {
            return null;
        }
        
        Tuple first = (Tuple) tuple.get(0);
        if (first == null) {
            return null;
        } 
        for (int i = 0; i < first.size(); i++) {
            if (!first.isNull(i)) {
                return first;
            }
        }
        return null;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return input;
    }
  
}
