package eu.dnetlib.iis.wf.importer.concept;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static eu.dnetlib.iis.wf.importer.VerificationUtils.verifyReport;
import static eu.dnetlib.iis.wf.importer.concept.ISLookupServiceBasedConceptImporter.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class ISLookupServiceBasedConceptImporterTest {

    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private ISLookupServiceBasedConceptImporter importer;

    @TempDir
    public File testFolder;
    
    @Mock
    private DataFileWriter<Concept> conceptWriter;
    
    @Captor
    private ArgumentCaptor<Concept> conceptCaptor;
    
    
    @BeforeEach
    public void init() {
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME,
                testFolder.getAbsolutePath() + File.separatorChar + "test.properties");
        
        Map<String, Path> output = new HashMap<>();
        output.put(PORT_OUT_CONCEPTS, new Path("/irrelevant/location/as/it/will/be/mocked"));
        this.portBindings = new PortBindings(Collections.emptyMap(), output);
        this.conf = new Configuration();
        this.parameters = new HashMap<>();
        this.parameters.put(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.concept.MockISLookupFacadeFactory");
        this.parameters.put(PARAM_IMPORT_CONTEXT_IDS_CSV, "fet");
        importer = new ISLookupServiceBasedConceptImporter() {
            
            @Override
            protected DataFileWriter<Concept> getWriter(FileSystem fs, PortBindings portBindings) throws IOException {
                return conceptWriter;
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
    public void testGetOutputPorts() {
        // execute
        Map<String, PortType> result = importer.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PORT_OUT_CONCEPTS));
        assertTrue(result.get(PORT_OUT_CONCEPTS) instanceof AvroPortType);
        assertSame(Concept.SCHEMA$, ((AvroPortType) result.get(PORT_OUT_CONCEPTS)).getSchema());
    }
    
    @Test
    public void testRunWithoutISLookupFacade() {
        // given
        parameters.remove(IMPORT_FACADE_FACTORY_CLASS);
        
        // execute
        assertThrows(ServiceFacadeException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunWithoutContextIds() {
        // given
        parameters.remove(PARAM_IMPORT_CONTEXT_IDS_CSV);
        
        // execute
        assertThrows(IllegalArgumentException.class, () -> importer.run(portBindings, conf, parameters));
    }
    
    @Test
    public void testRunEmptyResult() throws Exception {
        // given
        this.parameters.put(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.concept.EmptyResultsISLookupFacadeFactory");
        
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(conceptWriter, never()).append(any());
        verifyReport(0, CONCEPT_COUNTER_NAME);
    }
    
    @Test
    public void testRun() throws Exception {
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(conceptWriter, times(5)).append(conceptCaptor.capture());
        List<Concept> concepts = conceptCaptor.getAllValues();
        // in depth concept validation is not the subject of this test case
        assertEquals(5, concepts.size());
        assertNotNull(concepts.get(0));
        assertNotNull(concepts.get(1));
        assertNotNull(concepts.get(2));
        assertNotNull(concepts.get(3));
        assertNotNull(concepts.get(4));
        verifyReport(5, CONCEPT_COUNTER_NAME);
    }
    
}
