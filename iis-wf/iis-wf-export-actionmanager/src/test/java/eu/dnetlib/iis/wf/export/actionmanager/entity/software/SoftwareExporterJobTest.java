package eu.dnetlib.iis.wf.export.actionmanager.entity.software;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.IteratorUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionDeserializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.entity.software.SoftwareExportCounterReporter.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 */
@SlowTest
public class SoftwareExporterJobTest {

    private final SparkJobExecutor executor = new SparkJobExecutor();

    @TempDir
    public Path workingDir;

    private String inputDocumentToSoftwareUrlPath;
    private String inputDocumentMetadataPath;
    private String outputRelationPath;
    private String outputEntityPath;
    private String reportPath;

    private static final String RELATION_COLLECTED_FROM_KEY = "someRepo";
    
    @BeforeEach
    public void before() {
        inputDocumentToSoftwareUrlPath = workingDir.resolve("software_exporter").resolve("input_software").toString();
        inputDocumentMetadataPath = workingDir.resolve("software_exporter").resolve("input_metadata").toString();
        outputRelationPath = workingDir.resolve("software_exporter").resolve("output_relation").toString();
        outputEntityPath = workingDir.resolve("software_exporter").resolve("output_entity").toString();
        reportPath = workingDir.resolve("software_exporter").resolve("report").toString();
    }

    // ------------------------ TESTS --------------------------

    @Test
    public void exportSoftwareEntityBelowThreshold() throws IOException {

        // given
        String jsonInputSoftwareFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/export/actionmanager/entity/software/default/input/document_to_softwareurl_with_meta.json");
        String jsonInputMetadataFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/export/actionmanager/entity/software/default/input/document_metadata.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareFile, DocumentToSoftwareUrlWithMeta.class),
                inputDocumentToSoftwareUrlPath);

        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile,
                ExtractedDocumentMetadataMergedWithOriginal.class), inputDocumentMetadataPath);

        // execute
        executor.execute(buildJob("0.9"));

        // assert
        assertCountersInReport(0, 0, 0);

        List<AtomicAction<Software>> capturedEntityActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputEntityPath), text ->
                AtomicActionDeserializationUtils.deserializeAction(text.toString()));
        assertEquals(0, capturedEntityActions.size());

        List<AtomicAction<Relation>> capturedRelationActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputRelationPath), text ->
                AtomicActionDeserializationUtils.deserializeAction(text.toString()));
        assertEquals(0, capturedRelationActions.size());
    }

    @Test
    public void exportSoftwareEntity() throws IOException {

        // given
        String jsonInputSoftwareFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/export/actionmanager/entity/software/default/input/document_to_softwareurl_with_meta.json");
        String jsonInputMetadataFile = ClassPathResourceProvider
                .getResourcePath("eu/dnetlib/iis/wf/export/actionmanager/entity/software/default/input/document_metadata.json");

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareFile, DocumentToSoftwareUrlWithMeta.class),
                inputDocumentToSoftwareUrlPath);

        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile,
                ExtractedDocumentMetadataMergedWithOriginal.class), inputDocumentMetadataPath);

        // execute
        executor.execute(buildJob("$UNDEFINED$"));

        // assert
        assertCountersInReport(1, 1, 1);

        // verifying entities
        List<AtomicAction<Software>> capturedEntityActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputEntityPath), text ->
                AtomicActionDeserializationUtils.deserializeAction(text.toString()));
        assertEquals(1, capturedEntityActions.size());

        for (AtomicAction<Software> currentAction : capturedEntityActions) {
            verifyAction(currentAction, Software.class);
        }

        // verifying relations
        List<AtomicAction<Relation>> capturedRelationActions = IteratorUtils.toList(SequenceFileTextValueReader.fromFile(outputRelationPath), text ->
                AtomicActionDeserializationUtils.deserializeAction(text.toString()));
        assertEquals(2, capturedRelationActions.size());

        for (int i = 0; i < capturedRelationActions.size(); i++) {
            verifyAction(capturedRelationActions.get(i), Relation.class);
        }
    }

    // ------------------------ PRIVATE --------------------------

    private void assertCountersInReport(Integer expectedEntitiesCount, Integer expectedReferencesCount,
                                        Integer expectedDistinctPubsReferencesCount) throws IOException {
        List<ReportEntry> reportEntries = AvroTestUtils.readLocalAvroDataStore(reportPath);
        assertEquals(3, reportEntries.size());

        assertSame(ReportEntryType.COUNTER, reportEntries.get(0).getType());
        assertEquals(EXPORTED_SOFTWARE_ENTITIES_COUNTER, reportEntries.get(0).getKey().toString());
        assertEquals(expectedEntitiesCount, Integer.valueOf(reportEntries.get(0).getValue().toString()));

        assertSame(ReportEntryType.COUNTER, reportEntries.get(1).getType());
        assertEquals(SOFTWARE_REFERENCES_COUNTER, reportEntries.get(1).getKey().toString());
        assertEquals(expectedReferencesCount, Integer.valueOf(reportEntries.get(1).getValue().toString()));

        assertSame(ReportEntryType.COUNTER, reportEntries.get(2).getType());
        assertEquals(DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER, reportEntries.get(2).getKey().toString());
        assertEquals(expectedDistinctPubsReferencesCount, Integer.valueOf(reportEntries.get(2).getValue().toString()));
    }

    private static void verifyAction(AtomicAction<?> action, Class<?> clazz) {
        assertEquals(clazz, action.getClazz());
        assertNotNull(action.getPayload());
        assertEquals(clazz, action.getPayload().getClass());
        // comparing action payload is out of the scope of this test
    }

    private SparkJob buildJob(String trustLevelThreshold) {
        return SparkJobBuilder.create().setAppName("Spark Software Exporter").setMainClass(SoftwareExporterJob.class)
                .addArg("-inputDocumentToSoftwareUrlPath", inputDocumentToSoftwareUrlPath)
                .addArg("-inputDocumentMetadataPath", inputDocumentMetadataPath)
                .addArg("-trustLevelThreshold", trustLevelThreshold)
                .addArg("-collectedFromKey", RELATION_COLLECTED_FROM_KEY)
                .addArg("-outputEntityPath", outputEntityPath)
                .addArg("-outputRelationPath", outputRelationPath)
                .addArg("-outputReportPath", reportPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }

}
