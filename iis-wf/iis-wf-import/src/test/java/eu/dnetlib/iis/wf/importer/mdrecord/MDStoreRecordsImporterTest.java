package eu.dnetlib.iis.wf.importer.mdrecord;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_MDSTORE_IDS_CSV;
import static eu.dnetlib.iis.wf.importer.mdrecord.MDStoreRecordsImporter.COUNTER_NAME_TOTAL;
import static eu.dnetlib.iis.wf.importer.mdrecord.MDStoreRecordsImporter.COUNTER_NAME_SIZE_EXCEEDED;
import static eu.dnetlib.iis.wf.importer.mdrecord.MDStoreRecordsImporter.PORT_OUT_MDRECORDS;

import static eu.dnetlib.iis.wf.importer.VerificationUtils.verifyReport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;


/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class MDStoreRecordsImporterTest {

    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private MDStoreRecordsImporter importer;
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Mock
    private DataFileWriter<DocumentText> outputWriter;
    
    @Captor
    private ArgumentCaptor<DocumentText> outputCaptor;
    
    
    @Before
    public void init() throws Exception {
        
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getRoot().getAbsolutePath() + File.separatorChar + "test.properties");
        
        Map<String, Path> output = new HashMap<>();
        output.put(PORT_OUT_MDRECORDS, new Path("/irrelevant/location/as/it/will/be/mocked"));
        this.portBindings = new PortBindings(Collections.emptyMap(), output);
        this.conf = new Configuration();
        this.parameters = new HashMap<>();
        this.parameters.put(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.mdrecord.MockMDStoreFacadeFactory");
        this.parameters.put(IMPORT_MDSTORE_IDS_CSV, "irrelevant");
        importer = new MDStoreRecordsImporter() {
            
            @Override
            protected DataFileWriter<DocumentText> getWriter(FileSystem fs, PortBindings portBindings) throws IOException {
                return outputWriter;
            }
        };
    }
    
    // ----------------------------------- TESTS -----------------------------------
    
    @Test
    public void testGetInputPorts() throws Exception {
        // execute
        Map<String, PortType> result = importer.getInputPorts();
        
        // assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
    
    @Test
    public void testGetOutputPorts() throws Exception {
        // execute
        Map<String, PortType> result = importer.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PORT_OUT_MDRECORDS));
        assertTrue(result.get(PORT_OUT_MDRECORDS) instanceof AvroPortType);
        assertTrue(DocumentText.SCHEMA$ == ((AvroPortType)result.get(PORT_OUT_MDRECORDS)).getSchema());
    }
    
    @Test(expected=ServiceFacadeException.class)
    public void testRunWithoutMDStoreFacade() throws Exception {
        // given
        parameters.remove(IMPORT_FACADE_FACTORY_CLASS);
        
        // execute
        importer.run(portBindings, conf, parameters);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testRunWithoutMDStoreIds() throws Exception {
     // given
        parameters.remove(IMPORT_MDSTORE_IDS_CSV);
        
        // execute
        importer.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testRunEmptyResult() throws Exception {
        // given
        this.parameters.put(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.mdrecord.EmptyResultsMDStoreFacadeFactory");
        
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(outputWriter, never()).append(any());
        verifyReport(0, COUNTER_NAME_TOTAL);
        verifyReport(0, COUNTER_NAME_SIZE_EXCEEDED);
    }
    
    @Test
    public void testRun() throws Exception {
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(outputWriter, times(2)).append(outputCaptor.capture());
        List<DocumentText> results = outputCaptor.getAllValues();
        assertEquals(2, results.size());

        assertEquals("50|webcrawl____::000015093c397516c0b1b000f38982de", results.get(0).getId());
        assertTrue(StringUtils.isNotBlank(results.get(0).getText()));
        
        assertEquals("50|webcrawl____::000038b2e009d799643ba2b05a155877", results.get(1).getId());
        assertTrue(StringUtils.isNotBlank(results.get(1).getText()));
        
        verifyReport(2, COUNTER_NAME_TOTAL);
        verifyReport(0, COUNTER_NAME_SIZE_EXCEEDED);
    }
    
    @Test
    public void testRunSizeExceeded() throws Exception {
        // execute
        parameters.put(ImportWorkflowRuntimeParameters.IMPORT_MDSTORE_RECORD_MAXLENGTH, "1");
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(outputWriter, never()).append(outputCaptor.capture());
        
        verifyReport(0, COUNTER_NAME_TOTAL);
        verifyReport(3, COUNTER_NAME_SIZE_EXCEEDED);
    }
}
