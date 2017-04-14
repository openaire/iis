package eu.dnetlib.iis.wf.importer.stream.project;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.importer.VerificationUtils.verifyReport;
import static eu.dnetlib.iis.wf.importer.stream.project.StreamingProjectImporter.PORT_OUT_PROJECT;
import static eu.dnetlib.iis.wf.importer.stream.project.StreamingProjectImporter.PROJECT_COUNTER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
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
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class StreamingProjectImporterTest {

   
    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private StreamingProjectImporter importer = new StreamingProjectImporter();
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Mock
    private DataFileWriter<Project> projectWriter;
    
    @Captor
    private ArgumentCaptor<Project> projectCaptor;
    
    
    @Before
    public void init() throws Exception {
        
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getRoot().getAbsolutePath() + File.separatorChar + "test.properties");
        
        Map<String, Path> output = new HashMap<>();
        output.put(PORT_OUT_PROJECT, new Path("/irrelevant/location/as/it/will/be/mocked"));
        this.portBindings = new PortBindings(Collections.emptyMap(), output);
        this.conf = new Configuration();
        this.parameters = new HashMap<>();
        this.parameters.put(IMPORT_FACADE_FACTORY_CLASS, 
                "eu.dnetlib.iis.wf.importer.stream.project.StreamingFacadeMockFactory");

        importer = new StreamingProjectImporter() {
            
            @Override
            protected DataFileWriter<Project> getWriter(FileSystem fs, PortBindings portBindings) throws IOException {
                return projectWriter;
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
        assertNotNull(result.get(PORT_OUT_PROJECT));
        assertTrue(result.get(PORT_OUT_PROJECT) instanceof AvroPortType);
        assertTrue(Project.SCHEMA$ == ((AvroPortType)result.get(PORT_OUT_PROJECT)).getSchema());
    }
    
    @Test(expected=ServiceFacadeException.class)
    public void testRunWithoutStreamingFacade() throws Exception {
        // given
        parameters.remove(IMPORT_FACADE_FACTORY_CLASS);
        
        // execute
        importer.run(portBindings, conf, parameters);
    }
    
    @Test
    public void testRun() throws Exception {
        // execute
        importer.run(portBindings, conf, parameters);
        
        // assert
        verify(projectWriter, times(2)).append(projectCaptor.capture());
        List<Project> projects = projectCaptor.getAllValues();
        // in depth project validation is not the subject of this test case
        assertEquals(2, projects.size());
        assertNotNull(projects.get(0));
        assertNotNull(projects.get(1));
        verifyReport(2, PROJECT_COUNTER_NAME);
    }
    
}
