package eu.dnetlib.iis.common.javamapreduce.hack;

import eu.dnetlib.iis.common.javamapreduce.hack.MockOutputFormat.MockRecordWriter;
import eu.dnetlib.iis.common.schemas.Identifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static eu.dnetlib.iis.common.javamapreduce.hack.AvroMultipleOutputs.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class AvroMultipleOutputsTest {

    
    @Mock
    private RecordWriter<Identifier, Identifier> recordWriter;
    
    @Mock
    private OutputCommitter outputCommitter;
    
    @Mock
    private Job job;
    
    @Mock
    private TaskInputOutputContext<Identifier, Identifier, Identifier, Identifier> context;
    
    @Mock
    private TaskAttemptID taskAttemptId;
    
    @Mock
    private Counter counter;
    
    
    private Class<? extends OutputFormat<Identifier, Identifier>> outputFormatClass = InnerMockOutputFormat.class;
    
    
    // -------------------------------------- TESTS --------------------------------------
    
    @Test
    public void testAddNamedOutputInvalid() {
        // given
        String namedOutput = "invalid.name";

        // execute
        assertThrows(IllegalArgumentException.class, () ->
                AvroMultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, Identifier.SCHEMA$));
    }
    
    @Test
    public void testAddNamedOutput() {
        // given
        String namedOutput = "meta";
        Configuration conf = new Configuration();
        doReturn(conf).when(job).getConfiguration();
        
        // execute
        AvroMultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, Identifier.SCHEMA$);
        
        // assert
        assertEquals(" meta", conf.get(MULTIPLE_OUTPUTS));
        assertSame(outputFormatClass, conf.getClass(MO_PREFIX + namedOutput + FORMAT, OutputFormat.class));
        assertEquals(Identifier.SCHEMA$.toString(), conf.get(MO_PREFIX+namedOutput+".keyschema"));
        
    }
    
    @Test
    public void testAddNamedOutputWithValueSchemaSet() {
        // given
        String namedOutput = "metaI0";
        Configuration conf = new Configuration();
        doReturn(conf).when(job).getConfiguration();
        
        // execute
        AvroMultipleOutputs.addNamedOutput(job, namedOutput, outputFormatClass, 
                Identifier.SCHEMA$, Identifier.SCHEMA$);
        
        // assert
        assertEquals(' ' + namedOutput, conf.get(MULTIPLE_OUTPUTS));
        assertSame(outputFormatClass, conf.getClass(MO_PREFIX + namedOutput + FORMAT, OutputFormat.class));
        assertEquals(Identifier.SCHEMA$.toString(), conf.get(MO_PREFIX+namedOutput+".keyschema"));
        assertEquals(Identifier.SCHEMA$.toString(), conf.get(MO_PREFIX+namedOutput+".valueschema"));
    }
    
    
    @Test
    public void testSetCountersEnabled() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(job).getConfiguration();
        
        // execute
        AvroMultipleOutputs.setCountersEnabled(job, true);
        
        // assert
        assertTrue(conf.getBoolean(COUNTERS_ENABLED, false));
    }
    
    @Test
    public void testgetCountersEnabled() throws Exception {
        // given
        Configuration conf = new Configuration();
        doReturn(conf).when(job).getConfiguration();
        conf.setBoolean(COUNTERS_ENABLED, true);
        
        // execute & assert
        assertTrue(AvroMultipleOutputs.getCountersEnabled(job));
    }

    @Test
    public void testGetNamedOutput() throws Exception {
        // given
        String namedOutputMeta = "meta";
        String namedOutputFault = "fault";
        Configuration conf = new Configuration();
        conf.set(MULTIPLE_OUTPUTS, namedOutputMeta + ' ' + namedOutputFault);
        doReturn(conf).when(context).getConfiguration();
        AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        
        // execute
        Set<String> namedOutputs = multipleOutputs.getNamedOutputs();
        
        // assert
        assertEquals(2, namedOutputs.size());
        Iterator<String> it = namedOutputs.iterator();
        assertTrue(it.hasNext());
        assertEquals(namedOutputMeta, it.next());
        assertTrue(it.hasNext());
        assertEquals(namedOutputFault, it.next());
        assertFalse(it.hasNext());
    }
    
    @Test
    public void testGetRecordWriter() throws Exception {
        // given
        String baseFileName = "baseFileName";
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        doReturn(MockOutputFormat.class).when(context).getOutputFormatClass();
        AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        
        // execute
        RecordWriter<?, ?> recordWriter = multipleOutputs.getRecordWriter(context, baseFileName);
        
        // assert
        assertNotNull(recordWriter);
    }
    
    @Test
    public void testClose() throws Exception {
        // given
        String baseFileName = "baseFileName";
        Configuration conf = new Configuration();
        doReturn(conf).when(context).getConfiguration();
        doReturn(MockOutputFormat.class).when(context).getOutputFormatClass();
        AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        RecordWriter<?, ?> recordWriter = multipleOutputs.getRecordWriter(context, baseFileName);
        
        // execute
        multipleOutputs.close();
        
        // assert
        assertTrue(recordWriter instanceof MockRecordWriter);
        assertTrue(((MockRecordWriter)recordWriter).isClosed());
    }
    
    @Test
    public void testWrite() throws Exception {
        // given
        Identifier identifier = Identifier.newBuilder().setId("id-1").build();
        String namedOutputMeta = "meta";
        String namedOutputFault = "fault";
        
        Configuration conf = new Configuration();
        conf.set(MULTIPLE_OUTPUTS, namedOutputMeta + ' ' + namedOutputFault);
        conf.setClass(MO_PREFIX + namedOutputMeta + FORMAT, MockOutputFormat.class, OutputFormat.class);
        doReturn(MockOutputFormat.class).when(context).getOutputFormatClass();
        doReturn(conf).when(context).getConfiguration();
        doReturn(taskAttemptId).when(context).getTaskAttemptID();
        doReturn(new JobID()).when(taskAttemptId).getJobID();
        
        AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        RecordWriter<?, ?> recordWriter = multipleOutputs.getRecordWriter(context, namedOutputMeta);
        
        // execute
        multipleOutputs.write(namedOutputMeta, identifier);
        
        // assert
        assertNotNull(recordWriter);
        assertTrue(recordWriter instanceof MockRecordWriter);
        assertEquals(1, ((MockRecordWriter)recordWriter).getWrittenRecords().size());
        assertSame(identifier, ((MockRecordWriter) recordWriter).getWrittenRecords().get(0));
    }
    
    @Test
    public void testWriteWithCounter() throws Exception {
        // given
        Identifier identifier = Identifier.newBuilder().setId("id-1").build();
        String namedOutputMeta = "meta";
        String namedOutputFault = "fault";
        
        Configuration conf = new Configuration();
        conf.setBoolean(COUNTERS_ENABLED, true);
        conf.set(MULTIPLE_OUTPUTS, namedOutputMeta + ' ' + namedOutputFault);
        conf.setClass(MO_PREFIX + namedOutputMeta + FORMAT, MockOutputFormat.class, OutputFormat.class);
        doReturn(MockOutputFormat.class).when(context).getOutputFormatClass();
        doReturn(conf).when(context).getConfiguration();
        doReturn(counter).when(context).getCounter(COUNTERS_GROUP, namedOutputMeta);
        doReturn(taskAttemptId).when(context).getTaskAttemptID();
        doReturn(new JobID()).when(taskAttemptId).getJobID();
        
        AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        RecordWriter<?, ?> recordWriter = multipleOutputs.getRecordWriter(context, namedOutputMeta);
        
        // execute
        multipleOutputs.write(namedOutputMeta, identifier);
        
        // assert
        assertNotNull(recordWriter);
        verify(counter, times(1)).increment(1);
    }
    
    @Test
    public void testGetContext() throws Exception {
     // given
        String namedOutputMeta = "meta";
        String namedOutputFault = "fault";
        
        Configuration conf = new Configuration();
        conf.set(MULTIPLE_OUTPUTS, namedOutputMeta + ' ' + namedOutputFault);
        conf.set(MO_PREFIX + namedOutputMeta + ".keyschema", Identifier.SCHEMA$.toString());

        conf.setClass(MO_PREFIX + namedOutputMeta + FORMAT, MockOutputFormat.class, OutputFormat.class);
        doReturn(conf).when(context).getConfiguration();
        doReturn(taskAttemptId).when(context).getTaskAttemptID();
        doReturn(new JobID()).when(taskAttemptId).getJobID();
        
        AvroMultipleOutputs multipleOutputs = new AvroMultipleOutputs(context);
        
        // execute
        TaskAttemptContext attemptContext = multipleOutputs.getContext(namedOutputMeta);
        
        // assert
        assertNotNull(attemptContext);
        assertEquals(Identifier.SCHEMA$.toString(), attemptContext.getConfiguration().get(MO_PREFIX + namedOutputMeta + ".keyschema"));
        
    }
    
    // ---------------------------------------- INNER CLASS -------------------------------
    
    public class InnerMockOutputFormat extends OutputFormat<Identifier, Identifier> {

        @Override
        public RecordWriter<Identifier, Identifier> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            return recordWriter;
        }

        @Override
        public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
            return outputCommitter;
        }
        
    }
    
}
