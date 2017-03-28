package eu.dnetlib.iis.wf.collapsers;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import eu.dnetlib.iis.common.utils.AvroUtils;

/**
 * @author Dominika Tkaczyk
 */
public class CollapserReducer extends Reducer<AvroKey<String>, AvroValue<IndexedRecord>, AvroKey<IndexedRecord>, NullWritable> {
    
    private RecordCollapser<IndexedRecord, IndexedRecord> recordCollapser;
    
    private Class<IndexedRecord> inputSchemaClass;
    private Schema inputSchema;
    
    @SuppressWarnings("unchecked")
    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
        try {
            recordCollapser = 
                    (RecordCollapser<IndexedRecord, IndexedRecord>) getCollapserInstance(context, "record_collapser");
            recordCollapser.setup(context);
            
            String inputSchemaPath = context.getConfiguration().get("collapser.reducer.schema.class");
            inputSchema = AvroUtils.toSchema(inputSchemaPath);
            inputSchemaClass = (Class<IndexedRecord>) Class.forName(inputSchemaPath);
       
        } catch (Exception ex) {
            throw new IOException("Cannot set up collapser reducer!", ex);
        }
    }
    
    private static Object getCollapserInstance(Context context, String parameter) throws Exception {
        Class<?> collapserClass = Class.forName(context.getConfiguration().get(parameter));
        Constructor<?> collapserConstructor = collapserClass.getConstructor();
        return collapserConstructor.newInstance();
    }
    
    @Override
	public void reduce(AvroKey<String> key, Iterable<AvroValue<IndexedRecord>> values, Context context) 
            throws IOException, InterruptedException {
        Iterator<AvroValue<IndexedRecord>> iterator = values.iterator();
        List<IndexedRecord> objects = new ArrayList<IndexedRecord>();
        
        while (iterator.hasNext()) {            
            AvroValue<IndexedRecord> value = iterator.next();
            objects.add((IndexedRecord) AvroUtils.getCopy(value.datum(), inputSchema, inputSchemaClass));
        }
        
        List<IndexedRecord> collapsedList = recordCollapser.collapse(objects);
        for (IndexedRecord collapsed : collapsedList) {
            context.write(new AvroKey<IndexedRecord>(collapsed), NullWritable.get());
        }
	}

}
 