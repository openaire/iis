package eu.dnetlib.iis.wf.collapsers.basic;

import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import eu.dnetlib.iis.wf.collapsers.RecordCollapser;

/**
 * Abstract class for collapsing objects.
 *
 * @author Dominika Tkaczyk
 * @param <T> the type of input and output objects
 */
public abstract class SimpleCollapser<T extends IndexedRecord> implements RecordCollapser<T,T> {

    @Override
    public void setup(TaskAttemptContext context) {
    }

    /**
     * Collapses a list of objects.
     * 
     * @param objects a list of avro object to collapse
     * @return a list of collapsed objects
     */
    @Override
    public List<T> collapse(List<T> objects) {
        if (objects == null || objects.isEmpty()) {
            return null;
        }
        return collapseNonEmpty(objects);
    }

    /**
     * Collapses a non empty list of objects.
     * 
     * @param objects a non-empty list of objects to collapse
     * @return a list of collapsed objects
     */
    protected abstract List<T> collapseNonEmpty(List<T> objects);

}
