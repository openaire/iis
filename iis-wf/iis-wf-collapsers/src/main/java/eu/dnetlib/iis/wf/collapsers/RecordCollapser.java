package eu.dnetlib.iis.wf.collapsers;

import java.util.List;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;

/**
 * Record collapser interface.
 * 
 * @author Dominika Tkaczyk
 * @param <S> the type of input records
 * @param <T> the type of output collapsed records
 */
public interface RecordCollapser<S extends IndexedRecord, T extends IndexedRecord> {
    
    /**
     * Sets up all the needed parameters based on job configuration.
     * The method is called before the collapsing process starts.
     * 
     * @param configuration job configuration
     */
    void setup(Configuration configuration);
    
    /**
     * Collapses a list of objects.
     * 
     * @param objects a list of avro object to collapse
     * @return a list of collapsed objects
     */
    List<T> collapse(List<S> objects);

}
