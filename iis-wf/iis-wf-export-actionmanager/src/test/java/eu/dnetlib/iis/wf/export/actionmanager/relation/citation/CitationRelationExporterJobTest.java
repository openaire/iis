package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;
import eu.dnetlib.iis.common.spark.avro.AvroDatasetReader;
import eu.dnetlib.iis.export.schemas.Citations;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionDeserializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.relation.citation.Matchers.matchingAtomicAction;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CitationRelationExporterJobTest extends TestWithSharedSparkSession {

    @Test
    @DisplayName("Citation matching results are exported as atomic actions and report is generated")
    public void givenInputCitationsPath_whenRun_thenSerializedAtomicActionsAndReportsAreCreated(@TempDir Path rootInputPath,
                                                                                                @TempDir Path rootOutputPath) throws IOException {
        List<Citations> citationsList = Collections.singletonList(
                createCitations(
                        "DocumentId",
                        Collections.singletonList(
                                createCitationEntry("DestinationDocumentId", 1.0f)
                        ))
        );
        Path inputCitationsPath = rootInputPath.resolve("citations");
        new AvroDataFrameWriter(
                new AvroDataFrameSupport(spark()).createDataFrame(citationsList, Citations.SCHEMA$)).write(
                inputCitationsPath.toString()
        );
        float trustLevelThreshold = 0.5f;
        Path outputRelationPath = rootOutputPath.resolve("output");
        Path outputReportPath = rootOutputPath.resolve("report");

        CitationRelationExporterJob.main(new String[]{
                "-sharedSparkSession",
                "-inputCitationsPath", inputCitationsPath.toString(),
                "-outputRelationPath", outputRelationPath.toString(),
                "-outputReportPath", outputReportPath.toString(),
                "-trustLevelThreshold", Float.toString(trustLevelThreshold)
        });

        List<AtomicAction<Relation>> atomicActions = spark().sparkContext().sequenceFile(outputRelationPath.toString(), Text.class, Text.class)
                .toJavaRDD()
                .map(Tuple2::_2)
                .map(x -> AtomicActionDeserializationUtils.<Relation>deserializeAction(x.toString()))
                .collect();
        assertEquals(2, atomicActions.size());
        assertThat(atomicActions, hasItem(matchingAtomicAction(
                createAtomicAction("DocumentId", "DestinationDocumentId", OafConstants.REL_CLASS_CITES, 1.0f)
        )));
        assertThat(atomicActions, hasItem(matchingAtomicAction(
                createAtomicAction("DestinationDocumentId", "DocumentId", OafConstants.REL_CLASS_ISCITEDBY, 1.0f)
        )));

        assertEquals(1, HdfsTestUtils.countFiles(spark().sparkContext().hadoopConfiguration(), outputReportPath.toString(),
                x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        List<ReportEntry> reportEntries = new AvroDatasetReader(spark()).read(outputReportPath.toString(), ReportEntry.SCHEMA$, ReportEntry.class)
                .collectAsList();
        assertEquals(3, reportEntries.size());
        assertThat(reportEntries, hasItem(
                ReportEntryFactory.createCounterReportEntry("processing.citationMatching.relation.references", 1)
        ));
        assertThat(reportEntries, hasItem(
                ReportEntryFactory.createCounterReportEntry("processing.citationMatching.relation.cites.docs", 1)
        ));
        assertThat(reportEntries, hasItem(
                ReportEntryFactory.createCounterReportEntry("processing.citationMatching.relation.iscitedby.docs", 1)
        ));
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

    private static AtomicAction<Relation> createAtomicAction(String source,
                                                             String target,
                                                             String relClass,
                                                             Float confidenceLevel) {
        return new AtomicAction<>(Relation.class, createRelation(source, target, relClass, confidenceLevel));
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

}