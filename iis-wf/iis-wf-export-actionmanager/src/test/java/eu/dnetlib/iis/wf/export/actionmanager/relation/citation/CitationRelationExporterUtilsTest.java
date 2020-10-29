package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionDeserializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.CitationRelationExporterUtils.*;
import static org.apache.spark.sql.functions.udf;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CitationRelationExporterUtilsTest extends TestWithSharedSparkSession {

    @Nested
    public class ProcessCitationsTest {

        @Test
        @DisplayName("Processing returns empty dataset for input with empty citation entries")
        public void givenCitationsWithEmptyCitationEntries_whenProcessed_thenEmptyDataSetIsReturned() {
            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark());
            UserDefinedFunction isValidConfidenceLevel = udf((UDF1<Float, Boolean>) confidenceLevel -> true,
                    DataTypes.BooleanType);

            List<Relation> results = processCitations(avroDataFrameSupport.createDataFrame(Collections.emptyList(), Citations.SCHEMA$),
                    isValidConfidenceLevel).collectAsList();

            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Processing returns empty dataset for input without destination document id")
        public void givenCitationsWithNullDestinationDocumentId_whenProcessed_thenEmptyDataSetIsReturned() {
            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark());
            List<CitationEntry> citationEntries = Collections.singletonList(
                    createCitationEntry(null, 0.5f)
            );
            Citations citations = createCitations("DocumentId", citationEntries);
            UserDefinedFunction isValidConfidenceLevel = udf((UDF1<Float, Boolean>) confidenceLevel -> true,
                    DataTypes.BooleanType);
            Dataset<Row> citationsDF = avroDataFrameSupport.createDataFrame(Collections.singletonList(citations),
                    Citations.SCHEMA$);

            List<Relation> results = processCitations(citationsDF, isValidConfidenceLevel).collectAsList();

            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Processing returns empty dataset for input without confidence level")
        public void givenCitationsWithNullConfidenceLevel_whenProcessed_thenEmptyDataSetIsReturned() {
            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark());
            List<CitationEntry> citationEntries = Collections.singletonList(
                    createCitationEntry("DestinationDocumentId", null)
            );
            Citations citations = createCitations("DocumentId", citationEntries);
            UserDefinedFunction isValidConfidenceLevel = udf((UDF1<Float, Boolean>) confidenceLevel -> true,
                    DataTypes.BooleanType);
            Dataset<Row> citationsDF = avroDataFrameSupport.createDataFrame(Collections.singletonList(citations),
                    Citations.SCHEMA$);

            List<Relation> results = processCitations(citationsDF, isValidConfidenceLevel).collectAsList();

            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Processing returns empty dataset for input with invalid confidence level")
        public void givenCitationsWithConfidenceLevelBelowThreshold_whenProcessed_thenEmptyDataSetIsReturned() {
            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark());
            List<CitationEntry> citationEntries = Collections.singletonList(
                    createCitationEntry("DestinationDocumentId", 0.5f)
            );
            Citations citations = createCitations("DocumentId", citationEntries);
            UserDefinedFunction isValidConfidenceLevel = udf((UDF1<Float, Boolean>) confidenceLevel -> false,
                    DataTypes.BooleanType);
            Dataset<Row> citationsDF = avroDataFrameSupport.createDataFrame(Collections.singletonList(citations),
                    Citations.SCHEMA$);

            List<Relation> results = processCitations(citationsDF, isValidConfidenceLevel).collectAsList();

            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("Processing returns dataset with relations for valid input")
        public void givenOneCitationsRecord_whenProcessed_thenDataSetWithTwoRelationsIsReturned() {
            AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark());
            List<CitationEntry> citationEntries = Arrays.asList(
                    createCitationEntry("DestinationDocumentId", 0.9f),
                    createCitationEntry("DestinationDocumentId", 0.8f)
            );
            Citations citations = createCitations("DocumentId", citationEntries);
            UserDefinedFunction isValidConfidenceLevel = udf((UDF1<Float, Boolean>) confidenceLevel -> confidenceLevel > 0.5,
                    DataTypes.BooleanType);
            Dataset<Row> citationsDF = avroDataFrameSupport.createDataFrame(Collections.singletonList(citations),
                    Citations.SCHEMA$);

            List<Relation> results = processCitations(citationsDF, isValidConfidenceLevel).collectAsList();

            assertEquals(2, results.size());
            assertThat(results, hasItem(matchingRelation(
                    createRelation("DocumentId", "DestinationDocumentId", OafConstants.REL_CLASS_CITES, 0.9f))));
            assertThat(results, hasItem(matchingRelation(
                    createRelation("DestinationDocumentId", "DocumentId", OafConstants.REL_CLASS_ISCITEDBY, 0.9f))));
        }
    }

    @Test
    @DisplayName("Serialized actions are created from relations")
    public void givenRelations_whenCreatingToSerializedActions_thenSerializedActionsAreReturned() throws IOException, ClassNotFoundException {
        Relation relation = createRelation("source", "target", "relClass", 0.1f);
        Dataset<Relation> relations = spark().createDataset(Collections.singletonList(relation),
                Encoders.kryo(Relation.class));

        List<Text> results = relationsToSerializedActions(relations).collectAsList();

        assertEquals(1, results.size());
        assertThat(AtomicActionDeserializationUtils.getPayload(results.get(0).toString()), is(matchingRelation(relation)));
    }

    @Test
    @DisplayName("Report entries are created from relations")
    public void givenRelations_whenCreatingReportEntries_thenReportEntriesAreReturned() {
        Dataset<Relation> relations = spark().createDataset(Arrays.asList(
                createRelation("source", "target1", "relClass", 0.1f),
                createRelation("source", "target2", "relClass", 0.1f)
        ), Encoders.kryo(Relation.class));

        List<ReportEntry> results = relationsToReportEntries(spark(), relations).collectAsList();

        assertEquals(2, results.size());
        assertThat(results, hasItem(ReportEntryFactory.createCounterReportEntry("processing.citationMatching.relation.references", 2)));
        assertThat(results, hasItem(ReportEntryFactory.createCounterReportEntry("processing.citationMatching.relations.docs", 1)));
    }

    private static Citations createCitations(String documentId, List<CitationEntry> citationEntries) {
        return Citations.newBuilder()
                .setDocumentId(documentId)
                .setCitations(new GenericData.Array<>(Citations.SCHEMA$.getField("citations").schema(), citationEntries))
                .build();
    }

    private static CitationEntry createCitationEntry(String destinationDocumentId, Float confidenceLevel) {
        return CitationEntry.newBuilder()
                .setPosition(0)
                .setDestinationDocumentId(destinationDocumentId)
                .setConfidenceLevel(confidenceLevel)
                .setExternalDestinationDocumentIds(Collections.emptyMap())
                .build();
    }

    private static Relation createRelation(String source, String target, String relClass, Float confidenceLevel) {
        Relation relation = new Relation();
        relation.setRelType(OafConstants.REL_TYPE_RESULT_RESULT);
        relation.setSubRelType(OafConstants.SUBREL_TYPE_CITATION);
        relation.setRelClass(relClass);
        relation.setSource(source);
        relation.setTarget(target);
        relation.setDataInfo(BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel,
                "iis::document_referencedDocuments"));
        return relation;
    }

    private static Matcher<Relation> matchingRelation(Relation relation) {
        return new TypeSafeMatcher<Relation>() {
            @Override
            protected boolean matchesSafely(Relation item) {
                return relation.getRelType().equals(item.getRelType()) &&
                        relation.getSubRelType().equals(item.getSubRelType()) &&
                        relation.getRelClass().equals(item.getRelClass()) &&
                        relation.getSource().equals(item.getSource()) &&
                        relation.getTarget().equals(item.getTarget()) &&
                        Float.parseFloat(relation.getDataInfo().getTrust()) == Float.parseFloat(item.getDataInfo().getTrust()) &&
                        relation.getDataInfo().getInferenceprovenance().equals(item.getDataInfo().getInferenceprovenance());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("matching relation " + relation);
            }
        };
    }
}