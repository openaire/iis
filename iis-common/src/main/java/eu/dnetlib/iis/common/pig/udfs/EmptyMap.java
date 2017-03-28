package eu.dnetlib.iis.common.pig.udfs;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * Returns empty map.
 * 
 * @author mhorst
 */
public class EmptyMap extends EvalFunc<Map<String, String>> {

    @Override
    public Map<String, String> exec(Tuple tuple) throws IOException {
        return Collections.emptyMap();
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            return Utils.getSchemaFromString("m:map[chararray]");
        } catch (ParserException e) {
            return null;
        }
    }
    
}
