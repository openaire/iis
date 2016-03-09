package eu.dnetlib.iis.wf.collapsers;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

/**
 *
 * @author Dominika Tkaczyk
 */
public class CollapserUtils {

    /**
     * Checks, if all objects in the list have the same schema.
     */
    public static boolean haveEqualSchema(Collection<IndexedRecord> records) {
        if (records == null || records.isEmpty()) {
            return true;
        }
        Schema schema = records.iterator().next().getSchema();
        for (IndexedRecord record : records) {
            if (!schema.equals(record.getSchema())) {
                return false;
            }
        }
        return true;
    }
  
    /**
     * Checks, if the schema is an "envelope" schema.
     */
    public static boolean isEnvelopeSchema(Schema schema) {
        List<Field> fields = schema.getFields();
        if (fields.size() != 2) {
            return false;
        }
        if (schema.getField("origin") == null || schema.getField("data") == null) {
            return false;
        }
        return true;
    }
    
    /**
     * Extracts the "origin" value from the envelope record.
     */
    public static String getOriginValue(IndexedRecord record) {
        Schema schema = record.getSchema();
        Object origin = record.get(schema.getField("origin").pos());
        if (origin instanceof Utf8) {
            return ((Utf8) record.get(schema.getField("origin").pos())).toString();
        }
        return (String) record.get(schema.getField("origin").pos());
    }
    
    /**
     * Extracts the "data" part of the envelope record.
     */
    public static IndexedRecord getDataRecord(IndexedRecord record) {
        Schema schema = record.getSchema();
        if (schema.getField("data") == null) {
            return null;
        }
        return (IndexedRecord) record.get(schema.getField("data").pos());
    }
    
    /**
     * Extracts nested field value from a record.
     * 
     * @param record a record
     * @param fieldName a path of field names separated by a dot
     * @return 
     */
    public static <T> T getNestedFieldValue(IndexedRecord record, String fieldName) {
        if (record == null || fieldName == null) {
            return null;
        }
        IndexedRecord actRecord = record;
        String[] path = fieldName.split("\\.");
        for (int i = 0; i < path.length; i++) {
            Schema schema = actRecord.getSchema();
            if (schema.getField(path[i]) == null) {
                return null;
            }
            Object nextRecord = actRecord.get(schema.getField(path[i]).pos());
            if (i == path.length-1) {
                return (T) nextRecord;
            } else if (nextRecord instanceof IndexedRecord) {
                actRecord = (IndexedRecord) nextRecord;
            } else {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Returns the number of not null siginificant fields.
     * 
     * @param record input avro object
     * @param fields a list of significant field names
     * @return 
     */
    public static int getNumberOfFilledFields(IndexedRecord record, List<String> fields) {
        int number = 0;
        for (Field field: record.getSchema().getFields()) {
            if ((fields == null || fields.contains(field.name()))
                    && record.get(field.pos()) != null) {
                number++;
            }
        }
        
        return number;
    }
    
    static class NumberOfFilledDataFieldsComparator implements Comparator<IndexedRecord> {

        private List<String> fields;

        public NumberOfFilledDataFieldsComparator(List<String> fields) {
            this.fields = fields;
        }
        
        @Override
        public int compare(IndexedRecord t1, IndexedRecord t2) {
            return Integer.valueOf(getNumberOfFilledFields(t2, fields))
                        .compareTo(getNumberOfFilledFields(t1, fields));
        }
        
    }
    
    /**
     * Sorts records by the number of not null significant fields.
     */
    public static <T extends IndexedRecord> void sortByFilledDataFields(List<T> records, final List<String> fields) {
        Collections.sort(records, new NumberOfFilledDataFieldsComparator(fields));
    }
    
    /**
     * Merges two records by setting null fields in the base record.
     */
    public static <T extends IndexedRecord> T merge(T base, T update) {
        T ret = GenericData.get().deepCopy(base.getSchema(), base);
        for (Field field: base.getSchema().getFields()) {
            if (base.get(field.pos()) == null && update.get(field.pos()) != null) {
                ret.put(field.pos(), update.get(field.pos()));
            }
        }
        return ret;
    }
    
}
