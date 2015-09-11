package eu.dnetlib.iis.workflows.transformers.udfs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Merges two or more maps of type Map&lt;String, String&gt;. 
 * The output map contains all the keys from input maps. The value for a given key in the output map 
 * is the value taken from the first input map containing the given key.
 * 
 * @author Dominika Tkaczyk
 */
public class StringMapsMerger extends EvalFunc<Map<String, String>> {

    @Override
    public Map<String, String> exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        Map<String, String> merged = new HashMap<String, String>();

        for (int i = 0; i < tuple.size(); i++) {
            if (tuple.get(i) != null) {
                @SuppressWarnings("unchecked")
				Map<String, String> element = (Map<String, String>) tuple.get(i);
                for (Map.Entry<String, String> entry : element.entrySet()) {
                    if (merged.get(entry.getKey()) == null) {
                        merged.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
        if (merged.isEmpty()) {
            return null;
        }

        return merged;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return input.getField(0).schema;
        } catch (FrontendException ex) {
            return null;
        }
    }
    
}
