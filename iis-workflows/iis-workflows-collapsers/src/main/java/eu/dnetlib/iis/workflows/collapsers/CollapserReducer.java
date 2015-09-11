package eu.dnetlib.iis.workflows.collapsers;

import eu.dnetlib.iis.core.common.AvroUtils;
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

/**
 * @author Dominika Tkaczyk
 */
public class CollapserReducer extends Reducer<AvroKey<String>, AvroValue<IndexedRecord>, AvroKey<IndexedRecord>, NullWritable> {
    
    private RecordCollapser<IndexedRecord, IndexedRecord> recordCollapser;
    
    private Class inputSchemaClass;
    private Schema inputSchema;
    
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
        try {
            recordCollapser = 
                    (RecordCollapser) getCollapserInstance(context, "record_collapser");
            recordCollapser.setup(context.getConfiguration());
            
            String inputSchemaPath = context.getConfiguration().get("collapser.reducer.schema.class");
            inputSchema = AvroUtils.toSchema(inputSchemaPath);
            inputSchemaClass = Class.forName(inputSchemaPath);
       
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
            objects.add(AvroUtils.getCopy(value.datum(), inputSchema, inputSchemaClass));
        }
        
        List<IndexedRecord> collapsedList = recordCollapser.collapse(objects);
        for (IndexedRecord collapsed : collapsedList) {
            context.write(new AvroKey<IndexedRecord>(collapsed), NullWritable.get());
        }
	}

}
 