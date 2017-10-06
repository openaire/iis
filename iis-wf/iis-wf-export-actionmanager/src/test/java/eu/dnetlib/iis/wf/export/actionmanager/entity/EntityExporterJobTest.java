package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;

import datafu.com.google.common.collect.Lists;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.SequenceFileTextValueReader;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import pl.edu.icm.sparkutils.test.SparkJob;
import pl.edu.icm.sparkutils.test.SparkJobBuilder;
import pl.edu.icm.sparkutils.test.SparkJobExecutor;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class EntityExporterJobTest {

    private SparkJobExecutor executor = new SparkJobExecutor();
    
    private File workingDir;
    
    private String outputDirPath;
    
    private String reportDirPath;
    
    private String counterName = "export.entity.total";
    
    @Before
    public void before() {
        workingDir = Files.createTempDir();
        outputDirPath = workingDir + "/entity_exporter/output";
        reportDirPath = workingDir + "/entity_exporter/report";
    }
    
    
    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir);
        System.clearProperty(StaticFilterMock.SYSTEM_PROPERTY_RESOURCE_LOCATION);
    }
    
    //------------------------ TESTS --------------------------
    
    @Test(expected=RuntimeException.class)
    public void exportInvalidXsltLocation() throws IOException {
        
        // given
        String actionSetId = "actionset-id";
        System.setProperty(StaticFilterMock.SYSTEM_PROPERTY_RESOURCE_LOCATION, 
                "/eu/dnetlib/iis/wf/export/actionmanager/entity/dataset/default/input/datacite.xml");
        
        // execute
        executor.execute(buildJob(actionSetId, "eu/dnetlib/actionmanager/xslt/invalid.xslt"));

    }
    
    @Test
    public void exportEmptyEntity() throws IOException {
        
        // given
        String actionSetId = "actionset-id";

        // execute
        executor.execute(buildJob(actionSetId, "eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt"));
        
        // assert
        assertCounterInReport(0);
        
        List<AtomicAction> capturedActions = getActions();
        assertEquals(0, capturedActions.size());
    }
    
    @Test
    public void exportDatasetEntity() throws IOException {
        
        // given
        String actionSetId = "actionset-id";
        System.setProperty(StaticFilterMock.SYSTEM_PROPERTY_RESOURCE_LOCATION, 
                "/eu/dnetlib/iis/wf/export/actionmanager/entity/dataset/default/input/datacite.xml");

        // execute
        executor.execute(buildJob(actionSetId, "eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt"));
        
        // assert
        assertCounterInReport(1);
        
        List<AtomicAction> capturedActions = getActions();
        assertEquals(1, capturedActions.size());

        // in-depth action verification is not a subject for this test case
        verifyAction(capturedActions.get(0), actionSetId, 
                new String(InfoSpaceConstants.QUALIFIER_BODY, InfoSpaceConstants.ENCODING_UTF8), "result");
    }
   
    @Test
    public void exportDocumentEntity() throws IOException {
        
        // given
        String actionSetId = "actionset-id";
        System.setProperty(StaticFilterMock.SYSTEM_PROPERTY_RESOURCE_LOCATION, 
                "/eu/dnetlib/iis/wf/export/actionmanager/entity/document/default/input/document.xml");

        // execute
        executor.execute(buildJob(actionSetId, "eu/dnetlib/actionmanager/xslt/dmf2insertActions.xslt"));
        
        // assert
        assertCounterInReport(1);
        
        List<AtomicAction> capturedActions = getActions();
        assertEquals(1, capturedActions.size());

     // in-depth action verification is not a subject for this test case
        verifyAction(capturedActions.get(0), actionSetId, 
                new String(InfoSpaceConstants.QUALIFIER_BODY, InfoSpaceConstants.ENCODING_UTF8), "result");
    }
    
    //------------------------ PRIVATE --------------------------
    
    private void assertCounterInReport(Integer expectedCount) throws IOException {
        List<ReportEntry> reportEntries = AvroTestUtils.readLocalAvroDataStore(reportDirPath);
        assertEquals(1, reportEntries.size());
        assertTrue(ReportEntryType.COUNTER == reportEntries.get(0).getType());
        assertEquals(counterName, reportEntries.get(0).getKey().toString());
        assertEquals(expectedCount, Integer.valueOf(reportEntries.get(0).getValue().toString()));
    }
    
    /**
     * Verifies entity action.
     */
    private static void verifyAction(AtomicAction action, String actionSetId, String targetColumn, String targetColumnFamily) {
        assertEquals(actionSetId, action.getRawSet());
        assertTrue(action.getTargetValue().length > 0);
        assertTrue(StringUtils.isNotBlank(action.getRowKey()));
        assertTrue(StringUtils.isNotBlank(action.getTargetRowKey()));
        assertEquals(targetColumn, action.getTargetColumn());
        assertEquals(targetColumnFamily, action.getTargetColumnFamily());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getId(), action.getAgent().getId());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getName(), action.getAgent().getName());
        assertEquals(StaticConfigurationProvider.AGENT_DEFAULT.getType(), action.getAgent().getType());
    }
    
    private List<AtomicAction> getActions() throws IOException {
        List<AtomicAction> actions = Lists.newArrayList();
        
        try (SequenceFileTextValueReader it = new SequenceFileTextValueReader(
                new FileSystemPath(createLocalFileSystem(), new Path(new File(outputDirPath).getAbsolutePath())))) {
            while (it.hasNext()) {
                actions.add(AtomicAction.fromJSON(it.next().toString()));
            }
        }
        
        return actions;
    }
    
    private SparkJob buildJob(String actionSetId, String entityXSLTLocation) {
        return SparkJobBuilder.create()
                .setAppName("Spark Entity Exporter")
                .setMainClass(EntityExporterJob.class)
                .addArg("-inputRelationAvroPath", "irrelevant when using mocked filter")
                .addArg("-inputEntityAvroPath", "irrelevant when using mocked filter")
                .addArg("-entityFilterClassName", StaticFilterMock.class.getName())
                .addArg("-entityXSLTLocation", entityXSLTLocation)
                .addArg("-actionSetId", actionSetId)
                .addArg("-counterName", counterName)
                .addArg("-outputAvroPath", outputDirPath)
                .addArg("-outputReportPath", reportDirPath)
                .addJobProperty("spark.driver.host", "localhost")
                .build();
    }
    
    private static FileSystem createLocalFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        return FileSystem.get(conf);
    }
}

