package eu.dnetlib.iis.wf.export.actionmanager.entity;

import com.google.common.io.Files;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.export.actionmanager.RDDTestUtils;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.entity.SoftwareExportCounterReporter.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void exportSoftwareEntityBelowThreshold() throws IOException {
        
        // given
        String entityActionSetId = "entity-actionset-id";
        String relationActionSetId = "rel-actionset-id";
        
        String jsonInputSoftwareFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_to_softwareurl_with_meta.json";
        String jsonInputMetadataFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_metadata.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareFile, DocumentToSoftwareUrlWithMeta.class), 
                inputDocumentToSoftwareAvroPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile, ExtractedDocumentMetadataMergedWithOriginal.class), 
                inputDocumentMetadataAvroPath);

        // execute
        executor.execute(buildJob(entityActionSetId, relationActionSetId, "0.9"));
        
        // assert
        assertCountersInReport(0, 0, 0);
        
        List<AtomicAction> capturedEntityActions = RDDTestUtils
                .readValues(outputEntityDirPath, text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(0, capturedEntityActions.size());
        
        List<AtomicAction> capturedRelationActions = RDDTestUtils
                .readValues(outputRelationDirPath, text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(0, capturedRelationActions.size());
    }
    
    @Test
    public void exportSoftwareEntity() throws IOException {
        
        // given
        String entityActionSetId = "entity-actionset-id";
        String relationActionSetId = "rel-actionset-id";
        
        String jsonInputSoftwareFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_to_softwareurl_with_meta.json";
        String jsonInputMetadataFile = "src/test/resources/eu/dnetlib/iis/wf/export/actionmanager/software/data/document_metadata.json";
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputSoftwareFile, DocumentToSoftwareUrlWithMeta.class), 
                inputDocumentToSoftwareAvroPath);
        
        AvroTestUtils.createLocalAvroDataStore(
                JsonAvroTestUtils.readJsonDataStore(jsonInputMetadataFile, ExtractedDocumentMetadataMergedWithOriginal.class), 
                inputDocumentMetadataAvroPath);

        // execute
        executor.execute(buildJob(entityActionSetId, relationActionSetId, "$UNDEFINED$"));

        // assert
        assertCountersInReport(2, 3, 2);
        
        // in-depth action verification is not a subject for this test case
        String docId1 = "id-1";
        String docId2 = "id-2";
        String madisEntityId = SoftwareExporterJob.generateSoftwareEntityId("https://github.com/madgik/madis");
        String iisEntityId = SoftwareExporterJob.generateSoftwareEntityId("https://github.com/openaire/iis");
        
        // verifying entities
        List<AtomicAction> capturedEntityActions = RDDTestUtils
                .readValues(outputEntityDirPath, text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(2, capturedEntityActions.size());

        List<String> expectedTargetRowKeysToBeConsumed = new ArrayList<>(Arrays.asList(
                madisEntityId, iisEntityId));
        for (AtomicAction currentAction : capturedEntityActions) {
            verifyAction(currentAction, entityActionSetId, expectedTargetRowKeysToBeConsumed, 
                    new String(InfoSpaceConstants.QUALIFIER_BODY, InfoSpaceConstants.ENCODING_UTF8), "result");    
        }
        assertTrue(expectedTargetRowKeysToBeConsumed.isEmpty());

        // verifying relations
        List<AtomicAction> capturedRelationActions = RDDTestUtils
                .readValues(outputRelationDirPath, text -> AtomicAction.fromJSON(text.toString()));
        assertEquals(6, capturedRelationActions.size());

        String expectedColFam = RelType.resultResult.toString() + '_' + SubRelType.relationship + '_'
                + SoftwareExporterJob.REL_CLASS_ISRELATEDTO;
        
        String[] relationIdentifiers = {docId1, madisEntityId, docId2, madisEntityId, docId2, iisEntityId};
        expectedTargetRowKeysToBeConsumed = new ArrayList<>(Arrays.asList(relationIdentifiers));
        List<String> expectedColumnsToBeConsumed = new ArrayList<>(Arrays.asList(relationIdentifiers));
 
        for (int i=0 ; i < capturedRelationActions.size(); i++) {
            verifyAction(capturedRelationActions.get(i), relationActionSetId, 
                    expectedTargetRowKeysToBeConsumed, expectedColumnsToBeConsumed, expectedColFam);    
        }
        assertTrue(expectedTargetRowKeysToBeConsumed.isEmpty());
        assertTrue(expectedColumnsToBeConsumed.isEmpty());
    }
    
    //------------------------ PRIVATE --------------------------

    private void assertCountersInReport(Integer expectedEntitiesCount, 
            Integer expectedReferencesCount, Integer expectedDistictPubsRererencesCount) throws IOException {
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
    
    private static void verifyAction(AtomicAction action, String actionSetId, List<String> targetRowKeyCandidates, 
            String targetColumnFamily) {
        assertEquals(actionSetId, action.getRawSet());
        assertTrue(action.getTargetValue().length > 0);
        assertTrue(StringUtils.isNotBlank(action.getRowKey()));
        assertTrue(targetRowKeyCandidates.remove(action.getTargetRowKey()));
        assertEquals(targetColumnFamily, action.getTargetColumnFamily());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getId(), action.getAgent().getId());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getName(), action.getAgent().getName());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getType(), action.getAgent().getType());
    }
    
    /**
     * Verifies action.
     */
    private static void verifyAction(AtomicAction action, String actionSetId, List<String> targetRowKeyCandidates,
            String targetColumn, String targetColumnFamily) {
        verifyAction(action, actionSetId, targetRowKeyCandidates, targetColumnFamily);
        assertEquals(targetColumn, action.getTargetColumn());
    }
    
    /**
     * Verifies action.
     */
    private static void verifyAction(AtomicAction action, String actionSetId, List<String> targetRowKeyCandidates,
            List<String> targetColumnCandidates, String targetColumnFamily) {
        verifyAction(action, actionSetId, targetRowKeyCandidates, targetColumnFamily);
        assertTrue(targetColumnCandidates.remove(action.getTargetColumn()));
    }

    private SparkJob buildJob(
            String entityActionSetId, String relationActionSetId, String trustLevelThreshold) {
        return SparkJobBuilder.create()
                .setAppName("Spark Software Exporter")
                .setMainClass(SoftwareExporterJob.class)
                .addArg("-inputDocumentToSoftwareAvroPath", inputDocumentToSoftwareAvroPath)
                .addArg("-inputDocumentMetadataAvroPath", inputDocumentMetadataAvroPath)
                .addArg("-entityActionSetId", entityActionSetId)
                .addArg("-relationActionSetId", relationActionSetId)
                .addArg("-trustLevelThreshold", trustLevelThreshold)
                .addArg("-outputEntityPath", outputEntityDirPath)
                .addArg("-outputRelationPath", outputRelationDirPath)
                .addArg("-outputReportPath", reportDirPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
    
}
