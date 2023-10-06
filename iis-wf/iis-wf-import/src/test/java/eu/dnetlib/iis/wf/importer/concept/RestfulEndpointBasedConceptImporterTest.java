package eu.dnetlib.iis.wf.importer.concept;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Concept;
import eu.dnetlib.iis.wf.importer.concept.model.Context;
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

import com.google.gson.Gson;

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
public class RestfulEndpointBasedConceptImporterTest {

    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private RestfulEndpointBasedConceptImporter importer;

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
                "eu.dnetlib.iis.wf.importer.concept.MockContextStreamingFacadeFactory");
        this.parameters.put(PARAM_IMPORT_CONTEXT_IDS_CSV, "fet-fp7");
        importer = new RestfulEndpointBasedConceptImporter() {
            
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
    public void testRunWithoutFacade() {
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
                "eu.dnetlib.iis.wf.importer.concept.EmptyResultsContextStreamingFacadeFactory");
        
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(conceptWriter, never()).append(any());
        verifyReport(0, CONCEPT_COUNTER_NAME);
    }
    
    @Test
    public void testRunUnrecognizedContext() {
        // given
        // FIXME this test needs to be implemented once RESTful context API returns an appropriate HTTP error code 
        fail("NIY");
    }
    
    @Test
    public void testRun() throws Exception {
        // given
        Context[] expectedContexts = new Gson().fromJson(
                ClassPathResourceProvider.getResourceContent(MockContextStreamingFacadeFactory.fetProfileLocation),
                Context[].class);

        // execute
        importer.run(portBindings, conf, parameters);

        // assert
        verify(conceptWriter, times(expectedContexts.length)).append(conceptCaptor.capture());
        List<Concept> concepts = conceptCaptor.getAllValues();
        verifyReport(expectedContexts.length, CONCEPT_COUNTER_NAME);
        assertEquals(expectedContexts.length, concepts.size());

        for (int i = 0; i < expectedContexts.length; i++) {
            Context currentExpectedContext = expectedContexts[i];
            Concept currentConcept = concepts.get(i);

            assertEquals(currentExpectedContext.getId(), currentConcept.getId());
            assertEquals(currentExpectedContext.getLabel(), currentConcept.getLabel());

            assertEquals(currentExpectedContext.getParams().size(), currentConcept.getParams().size());
            for (int j = 0; j < currentExpectedContext.getParams().size(); j++) {
                assertEquals(currentExpectedContext.getParams().get(j).getName(),
                        currentConcept.getParams().get(j).getName());
                assertEquals(currentExpectedContext.getParams().get(j).getValue(),
                        currentConcept.getParams().get(j).getValue());
            }
        }

    }
    
}
