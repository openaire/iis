package eu.dnetlib.iis.common.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Deduplicates bag of tuples where tuple[0] is identifier and tuple[confidenceLevelPosition] is confidence level. 
 * Highest confidence level is picked when identifier duplicate is found. Identifiers are sorted lexicographically.
 *
 * @author mhorst
 */
public class DeduplicateIdsWithConfidence extends EvalFunc<DataBag> {

    /**
     * Confidence level position in tuple. First element is indexed with 0.
     */
    private final int confidenceLevelPosition;
    
    //------------------------ CONSTRUCTORS --------------------------

    public DeduplicateIdsWithConfidence() {
        this(1);
    }
    
    public DeduplicateIdsWithConfidence(int confidenceLevelPosition) {
        this.confidenceLevelPosition = confidenceLevelPosition;
    }
    
    public DeduplicateIdsWithConfidence(String confidenceLevelPosition) {
        this(Integer.valueOf(confidenceLevelPosition));
    }

    //------------------------ PUBLIC --------------------------
    
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        DataBag db = (DataBag) tuple.get(0);
        if (db == null) {
            return null;
        }
        if (db.size() > 1) {
            // deduplicating only if more than one element
            Iterator<Tuple> it = db.iterator();
            Map<String, Tuple> dedupMap = new TreeMap<String, Tuple>();
            while (it.hasNext()) {
                Tuple currentTuple = it.next();
                Tuple storedTuple = dedupMap.get(currentTuple.get(0));
                if (storedTuple != null) {
                    Float currentTupleConfidenceLevel = (Float) currentTuple.get(this.confidenceLevelPosition);
                    Float storedTupleConfidenceLevel = (Float) storedTuple.get(this.confidenceLevelPosition);
                    if (currentTupleConfidenceLevel != null) {
                        if (storedTupleConfidenceLevel != null) {
                            // storing the one with higher confidence level
                            if (currentTupleConfidenceLevel > storedTupleConfidenceLevel) {
                                dedupMap.put((String) currentTuple.get(0), currentTuple);
                            }
                        } else {
                            dedupMap.put((String) currentTuple.get(0), currentTuple);
                        }
                    }
                } else {
                    dedupMap.put((String) currentTuple.get(0), currentTuple);
                }
            }
            return BagFactory.getInstance().newDefaultBag(new ArrayList<Tuple>(dedupMap.values()));
        } else {
            return db;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return input;
    }

}
