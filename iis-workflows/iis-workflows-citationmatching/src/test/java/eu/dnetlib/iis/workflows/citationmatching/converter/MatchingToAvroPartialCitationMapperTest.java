package eu.dnetlib.iis.workflows.citationmatching.converter;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.PartialCitation;
import eu.dnetlib.iis.workflows.citationmatching.converter.MatchingToAvroPartialCitationMapper;
import eu.dnetlib.iis.workflows.citationmatching.converter.entity_id.CitEntityId;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import pl.edu.icm.coansys.citations.data.TextWithBytesWritable;

import java.io.IOException;
import java.util.List;

/**
 * @author Mateusz Fedoryszak (m.fedoryszak@icm.edu.pl)
 */
public class MatchingToAvroPartialCitationMapperTest {
    public static List<Pair<MatchableEntity, String>> getMatchingList() {
        return Lists.newArrayList(
                Pair.of(MatchableEntity.fromParameters("cit_1_1", null, null, null, null, null, "citation11"), "1.0:doc_11"),
                Pair.of(MatchableEntity.fromParameters("cit_1_2", null, null, null, null, null, "citation12"), "1.0:doc_12"),
                Pair.of(MatchableEntity.fromParameters("cit_2_1", null, null, null, null, null, "citation21"), "1.0:doc_21"),
                Pair.of(MatchableEntity.fromParameters("cit_2_2", null, null, null, null, null, "citation22"), "1.0:doc_22")
        );
    }

    public static List<PartialCitation> getPartialCitationList() {
        return Lists.newArrayList(
                new PartialCitation("1", 1, "11", 1.0f),
                new PartialCitation("1", 2, "12", 1.0f),
                new PartialCitation("2", 1, "21", 1.0f),
                new PartialCitation("2", 2, "22", 1.0f)
        );
    }

    @Test
    public void basicTest() throws IOException {
        MapDriver<TextWithBytesWritable, Text, AvroKey<String>, AvroValue<PartialCitation>> driver =
                MapDriver.newMapDriver(new MatchingToAvroPartialCitationMapper());

        Configuration conf = driver.getConfiguration();

        AvroSerialization.addToConfiguration(conf);
        AvroSerialization.setKeyWriterSchema(conf, Schema.create(Schema.Type.STRING));
        AvroSerialization.setValueWriterSchema(conf, PartialCitation.SCHEMA$);

        List<Pair<MatchableEntity, String>> data = getMatchingList();

        for (Pair<MatchableEntity, String> kv : data) {
            byte[] bytes = kv.getKey().data().toByteArray();
            driver.addInput(new TextWithBytesWritable(kv.getKey().id(), bytes), new Text(kv.getValue()));
        }

        for (PartialCitation cit : getPartialCitationList()) {
            driver.addOutput(
                    new AvroKey<String>(
                            new CitEntityId(cit.getSourceDocumentId().toString(), cit.getPosition()).toString()),
                    new AvroValue<PartialCitation>(cit));
        }

        driver.runTest(false);
    }
}
