package eu.dnetlib.iis.workflows.transformers.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Returns first not null Integer value from a tuple.
 *
 * @author Dominika Tkaczyk
 */
public class IntegerFirstNotEmpty extends EvalFunc<Integer> {

    @Override
    public Integer exec(Tuple tuple) throws IOException {
        if (tuple == null) {
            return null;
        }
        for (int i = 0; i < tuple.size(); i++) {
            Integer ret = (Integer) tuple.get(i);
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }
}
