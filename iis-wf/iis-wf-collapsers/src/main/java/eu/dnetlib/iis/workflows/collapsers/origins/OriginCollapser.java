package eu.dnetlib.iis.workflows.collapsers.origins;

import eu.dnetlib.iis.workflows.collapsers.CollapserUtils;
import eu.dnetlib.iis.workflows.collapsers.RecordCollapser;

import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;

/**
 * Abstract class for collapsing objects from various origins.
 *
 * @author Dominika Tkaczyk
 * @param <S> the type of input objects
 * @param <T> the type of output objects
 */
public abstract class OriginCollapser<S extends IndexedRecord, T extends IndexedRecord> implements RecordCollapser<S,T> {

    protected List<String> origins;
    
    /**
     * Sets the list of origins (used later during collapsing 
     * for determining the object "confidence").
     * 
     * Subclasses may override this method in order to read more parameters
     * from job configuration.
     * 
     * @param configuration job configuration
     */
    @Override
    public void setup(Configuration configuration) {
        origins = Arrays.asList(configuration.get("origins").split(","));
    }

    /**
     * Collapses a list of objects coming from various origins.
     * The method divides the list into separate lists based on
     * object origins and uses abstract {@link #collapseBetweenOrigins(map) collapseBetweenOrigins}
     * method to collapse. The Avro objects passed should be
     * "envelope" objects defined similarly to "record $ENVELOPE_NAME { $DATA_TYPE data;  string origin;}",
     * see also {@link #eu.dnetlib.iis.collapsers.CollapserUtils#isEnvelopeSchema(schema) CollapserUtils.isEnvelopeSchema}.
     * 
     * @param objects a list of input objects
     * @return a list of collapsed objects
     */
    @Override
    public List<T> collapse(List<S> objects) {
        if (objects == null || objects.isEmpty()) {
            return null;
        }
        Schema schema = objects.get(0).getSchema();
        if (!CollapserUtils.isEnvelopeSchema(schema)) {
            throw new IllegalArgumentException("Schema should have two fields: origin and data!");
        }
        
        Map<String, List<T>> recordMap = new HashMap<String, List<T>>();
        for (S record : objects) {
            String origin = CollapserUtils.getOriginValue(record);
            if (!origins.contains(origin)) {
                throw new IllegalArgumentException("Origin "+origin+" not present in the origins list!");
            }
            if (recordMap.get(origin) == null) {
                recordMap.put(origin, new ArrayList<T>());
            }

            try {
                recordMap.get(origin).add((T)CollapserUtils.getDataRecord(record));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Data field should have the same schema as the output objects!", e);
            }
        }
        
        return collapseBetweenOrigins(recordMap);
    }

    /**
     * Collapses objects from various origins.
     * 
     * @param objects a map (origin, a list of objects from origin)
     * @return a list of collapsed objects
     */
    protected abstract List<T> collapseBetweenOrigins(Map<String, List<T>> objects);

    public List<String> getOrigins() {
        return origins;
    }

    public void setOrigins(List<String> origins) {
        this.origins = origins;
    }
    
}
