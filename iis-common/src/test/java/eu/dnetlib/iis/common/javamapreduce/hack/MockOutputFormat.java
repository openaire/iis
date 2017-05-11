package eu.dnetlib.iis.common.javamapreduce.hack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import eu.dnetlib.iis.common.schemas.Identifier;

/**
 * @author mhorst
 *
 */
public class MockOutputFormat extends OutputFormat<Identifier, NullWritable> {

    @Override
    public RecordWriter<Identifier, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MockRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // does nothing
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new OutputCommitter() {

            @Override
            public void setupTask(TaskAttemptContext taskContext) throws IOException {
                // does nothing
            }
            
            @Override
            public void setupJob(JobContext jobContext) throws IOException {
                // does nothing
            }
            
            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
                return false;
            }
            
            @Override
            public void commitTask(TaskAttemptContext taskContext) throws IOException {
                // does nothing                
            }
            
            @Override
            public void abortTask(TaskAttemptContext taskContext) throws IOException {
                // does nothing                
            }
        };
    }
    
    public static class MockRecordWriter extends RecordWriter<Identifier, NullWritable> {

        private boolean closed;
        
        private List<Identifier> writtenRecords = new ArrayList<>();
        
        @Override
        public void write(Identifier key, NullWritable value) throws IOException, InterruptedException {
            writtenRecords.add(key);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            closed = true;
            
        }
        
        public boolean isClosed() {
            return closed;
        }

        public List<Identifier> getWrittenRecords() {
            return writtenRecords;
        }
        
    }
    
}