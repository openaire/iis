package eu.dnetlib.iis.common.javamapreduce.hack;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class DelegatingInputFormat<K, V> extends org.apache.hadoop.mapreduce.lib.input.DelegatingInputFormat<K, V> {
    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        SchemaSetter.set(context.getConfiguration());
        return super.createRecordReader(split, context);
    }
}
