package eu.dnetlib.iis.workflows.transformers.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Returns first not null and not empty String value from a tuple.
 *
 * @author Dominika Tkaczyk
 */
public class StringFirstNotEmpty extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null) {
            return null;
        }
        for (int i = 0; i < input.size(); i++) {
            String ret = (String) input.get(i);
            if (ret != null && !ret.isEmpty()) {
                return ret;
            }
        }
        return null;
    }
}
