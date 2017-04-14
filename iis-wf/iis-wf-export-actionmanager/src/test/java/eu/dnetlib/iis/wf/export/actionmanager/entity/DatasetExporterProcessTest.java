package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static eu.dnetlib.iis.wf.export.actionmanager.entity.VerificationUtils.verifyAction;
import static eu.dnetlib.iis.wf.export.actionmanager.entity.VerificationUtils.verifyReport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.DatasetToMDStore;
import eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.export.actionmanager.api.ActionManagerServiceFacade;
import eu.dnetlib.iis.wf.export.actionmanager.entity.facade.MockMDStoreFacadeFactory;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DatasetExporterProcessTest {

    
    private final String actionSetId = "testActionSetId";
    
    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private DatasetExporterProcess process;
    
  
    @Mock
    private ActionManagerServiceFacade actionManagerServiceFacade;
    
    @Mock
    private CloseableIterator<DatasetToMDStore> inputRecordsIterator;
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Captor
    private ArgumentCaptor<List<AtomicAction>> actionsCaptor;
    
    @Before
    public void init() throws Exception {
        System.setProperty(AbstractEntityExporterProcess.OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getRoot().getAbsolutePath() + File.separatorChar + "test.properties");
        
        Map<String, Path> input = new HashMap<>();
        input.put(AbstractEntityExporterProcess.PORT_INPUT, new Path("/irrelevant/location/as/it/will/be/mocked"));
        this.portBindings = new PortBindings(input, Collections.emptyMap());
        this.conf = new Configuration();
        this.parameters = new HashMap<>();
        this.parameters.put(ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID, actionSetId);
        this.parameters.put(AbstractEntityExporterProcess.MDSTORE_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.export.actionmanager.entity.facade.MockMDStoreFacadeFactory");
        this.process = new DatasetExporterProcess() {
            
            @Override
            protected ActionManagerServiceFacade buildActionManager(Configuration conf, Map<String, String> parameters)
                    throws IOException {
                return actionManagerServiceFacade;
            }
            
            @Override
            protected CloseableIterator<DatasetToMDStore> getIterator(Path inputPath, Configuration conf) throws IOException {
                return inputRecordsIterator;
            }
            
        };
    }
    
    @Test
    public void testGetInputPorts() throws Exception {
        // execute
        Map<String, PortType> result = process.getInputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(DatasetExporterProcess.PORT_INPUT));
        assertTrue(result.get(DatasetExporterProcess.PORT_INPUT) instanceof AvroPortType);
        assertTrue(DatasetToMDStore.SCHEMA$ == ((AvroPortType)result.get(DatasetExporterProcess.PORT_INPUT)).getSchema());
    }
    
    @Test
    public void testGetOutputPorts() throws Exception {
        // execute
        Map<String, PortType> result = process.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testRunWithoutActionSetId() throws Exception {
        // given
        parameters.remove(ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID);
        
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testRunWithoutMDStoreFacade() throws Exception {
        // given
        parameters.remove(AbstractEntityExporterProcess.MDSTORE_FACADE_FACTORY_CLASS);
        
        // execute
        process.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testRunWithEmptyInput() throws Exception {
        // given
        doReturn(false).when(inputRecordsIterator).hasNext();

        // execute
        process.run(portBindings, conf, parameters);
        
        // assert
        verify(actionManagerServiceFacade, never()).storeActions(Mockito.any());
        verifyReport(0, 0);
        
    }

    @Test
    public void testRunWithMissingRecord() throws Exception {
     // given
        String mdStoreId = "mdStoreId";
        String datasetId = "datasetId";
        DatasetToMDStore inputRecord = new DatasetToMDStore(datasetId, mdStoreId);
        doReturn(true).doReturn(false).when(inputRecordsIterator).hasNext();
        doReturn(inputRecord).when(inputRecordsIterator).next();
        
        // execute
        process.run(portBindings, conf, parameters);
        
        verify(actionManagerServiceFacade, never()).storeActions(Mockito.any());
        verifyReport(0, 1);
    }
    
    @Test
    public void testRun() throws Exception {
        // given
        String mdStoreId = "mdStoreId";
        String datasetId = "datasetId";
        DatasetToMDStore inputRecord = new DatasetToMDStore(datasetId, mdStoreId);
        this.parameters.put(MockMDStoreFacadeFactory.buildParameterKey(mdStoreId, datasetId), 
                        "/eu/dnetlib/iis/wf/export/actionmanager/entity/dataset/default/input/datacite.xml");
        doReturn(true).doReturn(false).when(inputRecordsIterator).hasNext();
        doReturn(inputRecord).when(inputRecordsIterator).next();
        
        // execute
        process.run(portBindings, conf, parameters);
        
        // assert
        verify(actionManagerServiceFacade).storeActions(actionsCaptor.capture());
        List<AtomicAction> capturedActions = actionsCaptor.getValue();
        assertNotNull(capturedActions);
        assertEquals(4, capturedActions.size());
        
        // in-depth action verification is not a subject for this test case
        verifyAction(capturedActions.get(0), actionSetId, 
                new String(HBaseConstants.QUALIFIER_BODY, HBaseConstants.STATIC_FIELDS_ENCODING_UTF8), "result");
        verifyAction(capturedActions.get(1), actionSetId, 
                new String(HBaseConstants.QUALIFIER_BODY, HBaseConstants.STATIC_FIELDS_ENCODING_UTF8), "person");
        verifyAction(capturedActions.get(2), actionSetId, 
                capturedActions.get(0).getTargetRowKey(), "personResult_authorship_isAuthorOf");
        verifyAction(capturedActions.get(3), actionSetId, 
                capturedActions.get(1).getTargetRowKey(), "personResult_authorship_hasAuthor");
        
        verifyReport(1, 0);
    }
    
}