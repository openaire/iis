package eu.dnetlib.iis.workflows.citationmatching.converter;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.workflows.citationmatching.converter.AvroDocumentMetadataToProtoBufMatchableEntityMapper;
import eu.dnetlib.iis.workflows.citationmatching.converter.entity_id.DocEntityId;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class AvroDocumentMetadataToProtoBufMatchableEntityMapperTest {
    @Test
    public void basicTest() throws IOException {
//        MapDriver<AvroKey<DocumentMetadata>, NullWritable, Text, BytesWritable> driver =
//                MapDriver.newMapDriver(new AvroDocumentMetadataToProtoBufMatchableEntityMapper());
//
//        Configuration conf = driver.getConfiguration();
//
//        AvroSerialization.addToConfiguration(conf);
//        AvroSerialization.setKeyWriterSchema(conf, DocumentMetadata.SCHEMA$);
//        AvroSerialization.setValueWriterSchema(conf, Schema.create(Schema.Type.NULL));
//
//        List<DocumentMetadata> data = DocumentAvroDatastoreProducer.getDocumentMetadataList();
//
//        for (DocumentMetadata meta : data) {
//            driver.addInput(new AvroKey<DocumentMetadata>(meta), NullWritable.get());
//        }
//
//        List<Pair<Text, BytesWritable>> results = driver.run();
//
//        assertEquals(new DocEntityId("1").toString(), results.get(0).getFirst().toString());
//        assertEquals(new DocEntityId("2").toString(), results.get(1).getFirst().toString());
    }
}
