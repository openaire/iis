package eu.dnetlib.iis.wf.documentssimilarity.converter;

import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class TsvToAvroMapper extends Mapper<Writable, Text, AvroKey<DocumentSimilarity>, NullWritable> {
    
    private static final Logger log = Logger.getLogger(TsvToAvroMapper.class);

    @Override
    protected void map(Writable ignore, Text data, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = data.toString().split("\\t");
            DocumentSimilarity similarity = new DocumentSimilarity();
            similarity.setDocumentId(fields[0]);
            similarity.setOtherDocumentId(fields[1]);
            similarity.setSimilarity(Float.parseFloat(fields[2]));
            context.write(new AvroKey<DocumentSimilarity>(similarity), NullWritable.get());
        } catch (Exception e) {
            log.error(e);
        }
    }
}
