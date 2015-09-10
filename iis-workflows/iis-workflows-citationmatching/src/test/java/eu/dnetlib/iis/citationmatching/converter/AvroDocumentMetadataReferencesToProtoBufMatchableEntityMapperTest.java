package eu.dnetlib.iis.citationmatching.converter;

import eu.dnetlib.iis.citationmatching.converter.entity_id.CitEntityId;
import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
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
public class AvroDocumentMetadataReferencesToProtoBufMatchableEntityMapperTest {

    // TODO MiconCodeReview: Overall this way of testing is hugely advantageous compared to testing with MiniOozie,
    // TODO MiconCodeReview: due to a very short time of test execution.

    // TODO MiconCodeReview: Might be useful, to extract a class AvroMapReduceTest test utility class that would allow
    // TODO MiconCodeReview: easy testing of Mappers and Reducers accepting Avro inputs and outputs.

    // TODO MiconCodeReview: Nice feature would be to load input and output data from *.json files, just like in the
    // TODO MiconCodeReview: workflow tests using MiniOozie in icm-iis-transformers or icm-iis-collapsers.

    // TODO MiconCodeReview: Usage could look like that:
    // TODO MiconCodeReview:   AvroMapReduceTest
    // TODO MiconCodeReview:           .forMapper(new AvroDocumentMetadataReferencesToProtoBufMatchableEntityMapper())
    // TODO MiconCodeReview:           .givenInput(inputJsonFilePath)
    // TODO MiconCodeReview:           .withSchema(inputSchema)
    // TODO MiconCodeReview:           .expectOutput(outputJsonFilePath)
    // TODO MiconCodeReview:           .withSchema(outputSchema)
    // TODO MiconCodeReview:           .runTestIgnoringOutputOrdering()

    @Test
    public void basicTest() throws IOException {
        MapDriver<AvroKey<DocumentMetadata>, NullWritable, Text, BytesWritable> driver =
                MapDriver.newMapDriver(new AvroDocumentMetadataReferencesToProtoBufMatchableEntityMapper());

        Configuration conf = driver.getConfiguration();

        AvroSerialization.addToConfiguration(conf);
        AvroSerialization.setKeyWriterSchema(conf, DocumentMetadata.SCHEMA$);
        AvroSerialization.setValueWriterSchema(conf, Schema.create(Schema.Type.NULL));

        List<DocumentMetadata> data = DocumentAvroDatastoreProducer.getDocumentMetadataList();

        for (DocumentMetadata meta : data) {
            driver.addInput(new AvroKey<DocumentMetadata>(meta), NullWritable.get());
        }

        List<Pair<Text, BytesWritable>> results = driver.run();

        assertEquals(new CitEntityId("1", 1).toString(), results.get(0).getFirst().toString());
        assertEquals(new CitEntityId("1", 2).toString(), results.get(1).getFirst().toString());
        assertEquals(new CitEntityId("2", 1).toString(), results.get(2).getFirst().toString());
        assertEquals(new CitEntityId("2", 2).toString(), results.get(3).getFirst().toString());
    }
}
