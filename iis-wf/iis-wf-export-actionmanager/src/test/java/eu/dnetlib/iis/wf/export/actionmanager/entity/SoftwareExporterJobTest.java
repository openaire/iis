package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static eu.dnetlib.iis.wf.export.actionmanager.entity.SoftwareExportCounterReporter.DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER;
import static eu.dnetlib.iis.wf.export.actionmanager.entity.SoftwareExportCounterReporter.EXPORTED_SOFTWARE_ENTITIES_COUNTER;
import static eu.dnetlib.iis.wf.export.actionmanager.entity.SoftwareExportCounterReporter.SOFTWARE_REFERENCES_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.common.utils.ListTestUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class SoftwareExporterJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();

    private File workingDir;

    private String inputDocumentToSoftwareAvroPath;

    private String inputDocumentMetadataAvroPath;

    private String outputEntityDirPath;

    private String outputRelationDirPath;

    private String reportDirPath;

    @Before
    public void before() {
        workingDir = Files.createTempDir();
        inputDocumentToSoftwareAvroPath = workingDir + "/software_exporter/input_software";
        inputDocumentMetadataAvroPath = workingDir + "/software_exporter/input_metadata";
        outputEntityDirPath = workingDir + "/software_exporter/output_entity";
        outputRelationDirPath = workingDir + "/software_exporter/output_relation";
        reportDirPath = workingDir + "/software_exporter/report";
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
    }

    // ------------------------ TESTS --------------------------

    @Test
    public void exportSoftwareEntityBelowThreshold() throws IOException {

        // given
        String jsonInputSoftwareFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_to_softwareurl_with_meta.json";
        String jsonInputMetadataFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_metadata.json";

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareFile, DocumentToSoftwareUrlWithMeta.class),
                inputDocumentToSoftwareAvroPath);

        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile,
                ExtractedDocumentMetadataMergedWithOriginal.class), inputDocumentMetadataAvroPath);

        // execute
        executor.execute(buildJob("0.9"));

        // assert
        assertCountersInReport(0, 0, 0);

        List<AtomicAction<Software>> capturedEntityActions = ListTestUtils.readValues(outputEntityDirPath, text -> {
            try {
                return AtomicActionSerDeUtils.deserializeAction(text.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(0, capturedEntityActions.size());

        List<AtomicAction<Relation>> capturedRelationActions = ListTestUtils.readValues(outputRelationDirPath, text -> {
            try {
                return AtomicActionSerDeUtils.deserializeAction(text.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(0, capturedRelationActions.size());
    }

    @Test
    public void exportSoftwareEntity() throws IOException {

        // given
        String jsonInputSoftwareFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_to_softwareurl_with_meta.json";
        String jsonInputMetadataFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_metadata.json";

        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareFile, DocumentToSoftwareUrlWithMeta.class),
                inputDocumentToSoftwareAvroPath);

        AvroTestUtils.createLocalAvroDataStore(JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile,
                ExtractedDocumentMetadataMergedWithOriginal.class), inputDocumentMetadataAvroPath);

        // execute
        executor.execute(buildJob("$UNDEFINED$"));

        // assert
        assertCountersInReport(2, 3, 2);

        // verifying entities
        List<AtomicAction<Software>> capturedEntityActions = ListTestUtils.readValues(outputEntityDirPath, text -> {
            try {
                return AtomicActionSerDeUtils.deserializeAction(text.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(2, capturedEntityActions.size());

        for (AtomicAction<Software> currentAction : capturedEntityActions) {
            verifyAction(currentAction, Software.class);
        }

        // verifying relations
        List<AtomicAction<Relation>> capturedRelationActions = ListTestUtils.readValues(outputRelationDirPath, text -> {
            try {
                return AtomicActionSerDeUtils.deserializeAction(text.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(6, capturedRelationActions.size());

        for (int i = 0; i < capturedRelationActions.size(); i++) {
            verifyAction(capturedRelationActions.get(i), Relation.class);
        }
    }

    // ------------------------ PRIVATE --------------------------

    private void assertCountersInReport(Integer expectedEntitiesCount, Integer expectedReferencesCount,
            Integer expectedDistictPubsRererencesCount) throws IOException {
        List<ReportEntry> reportEntries = AvroTestUtils.readLocalAvroDataStore(reportDirPath);
        assertEquals(3, reportEntries.size());

        assertTrue(ReportEntryType.COUNTER == reportEntries.get(0).getType());
        assertEquals(EXPORTED_SOFTWARE_ENTITIES_COUNTER, reportEntries.get(0).getKey().toString());
        assertEquals(expectedEntitiesCount, Integer.valueOf(reportEntries.get(0).getValue().toString()));

        assertTrue(ReportEntryType.COUNTER == reportEntries.get(1).getType());
        assertEquals(SOFTWARE_REFERENCES_COUNTER, reportEntries.get(1).getKey().toString());
        assertEquals(expectedReferencesCount, Integer.valueOf(reportEntries.get(1).getValue().toString()));

        assertTrue(ReportEntryType.COUNTER == reportEntries.get(2).getType());
        assertEquals(DISTINCT_PUBLICATIONS_WITH_SOFTWARE_REFERENCES_COUNTER, reportEntries.get(2).getKey().toString());
        assertEquals(expectedDistictPubsRererencesCount, Integer.valueOf(reportEntries.get(2).getValue().toString()));
    }

    private static void verifyAction(AtomicAction<?> action, Class<?> clazz) {
        assertEquals(clazz, action.getClazz());
        assertNotNull(action.getPayload());
        assertEquals(clazz, action.getPayload().getClass());
        // comparing action payload is out of the scope of this test
    }

    private SparkJob buildJob(String trustLevelThreshold) {
        return SparkJobBuilder.create().setAppName("Spark Software Exporter").setMainClass(SoftwareExporterJob.class)
                .addArg("-inputDocumentToSoftwareAvroPath", inputDocumentToSoftwareAvroPath)
                .addArg("-inputDocumentMetadataAvroPath", inputDocumentMetadataAvroPath)
                .addArg("-trustLevelThreshold", trustLevelThreshold).addArg("-outputEntityPath", outputEntityDirPath)
                .addArg("-outputRelationPath", outputRelationDirPath).addArg("-outputReportPath", reportDirPath)
                .addJobProperty("spark.driver.host", "localhost").build();
    }

}
