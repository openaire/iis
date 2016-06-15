package eu.dnetlib.iis.common.pig.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
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
    
    /**
     * @param confidenceLevelPosition {@link String} representation of {@link Integer} value.
     * Required by PIG.
     */
    public DeduplicateIdsWithConfidence(String confidenceLevelPosition) {
        this(Integer.valueOf(confidenceLevelPosition));
    }

    //------------------------ PUBLIC --------------------------
    
    /**
     * Deduplicates tuples by grouping them by identifier stored in tuple[0] and picking the one with highest 
     * confidence level stored in tuple[confidenceLevelPosition].
     * 
     * @param tuple {@link DataBag} holding group of tuples to be deduplicated
     */
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        DataBag inputTuples = (DataBag) tuple.get(0);
        if (inputTuples != null && inputTuples.size() > 1) {
            // deduplicating only if more than one element
            Map<String, Tuple> deduplicatedTuplesMap = new TreeMap<String, Tuple>();
            Iterator<Tuple> inputTuplesIterator = inputTuples.iterator();
            while (inputTuplesIterator.hasNext()) {
                Tuple currentTuple = inputTuplesIterator.next();
                updateStoredTupleWhenConfidenceLevelHigher(currentTuple, deduplicatedTuplesMap);
            }
            return BagFactory.getInstance().newDefaultBag(new ArrayList<Tuple>(deduplicatedTuplesMap.values()));
        } else {
            return inputTuples;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return input;
    }

    //------------------------ PRIVATE --------------------------
    /**
     * Stores <code>newTuple</code> in <code>deduplicatedTuplesMap</code> when its confidence level is
     * higher than already stored tuple or when tuple for given identifier (stored as first tuple element) was not stored yet. 
     */
    private void updateStoredTupleWhenConfidenceLevelHigher(Tuple newTuple, Map<String, Tuple> deduplicatedTuplesMap) throws ExecException {
        String tupleId = (String) newTuple.get(0);
        Tuple storedTuple = deduplicatedTuplesMap.get(tupleId);
        if (storedTuple != null) {
            Float newTupleConfidenceLevel = (Float) newTuple.get(this.confidenceLevelPosition);
            Float storedTupleConfidenceLevel = (Float) storedTuple.get(this.confidenceLevelPosition);
            if (newTupleConfidenceLevel != null &&
                    (storedTupleConfidenceLevel == null || newTupleConfidenceLevel > storedTupleConfidenceLevel)) {
                // replacing stored tuple with new tuple when confidence level is higher or when stored tuple didn't have it defined
                deduplicatedTuplesMap.put(tupleId, newTuple);
            }
        } else {
            deduplicatedTuplesMap.put(tupleId, newTuple);
        }
    }
}
