package eu.dnetlib.iis.wf.importer.stream.project;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_FACADE_FACTORY_CLASS;
import static eu.dnetlib.iis.wf.importer.VerificationUtils.verifyReport;
import static eu.dnetlib.iis.wf.importer.stream.project.StreamingProjectImporter.PORT_OUT_PROJECT;
import static eu.dnetlib.iis.wf.importer.stream.project.StreamingProjectImporter.PROJECT_COUNTER_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class StreamingProjectImporterTest {

   
    private PortBindings portBindings;
    
    private Configuration conf;
    
    private Map<String, String> parameters;
    
    private StreamingProjectImporter importer = new StreamingProjectImporter();
    
    public File testFolder;
    
    @Mock
    private DataFileWriter<Project> projectWriter;
    
    @Captor
    private ArgumentCaptor<Project> projectCaptor;
    
    
    @BeforeEach
    public void init() throws Exception {
        testFolder = Files.createTempDirectory(this.getClass().getSimpleName()).toFile();

        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getAbsolutePath() + File.separatorChar + "test.properties");
        
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
    public void testGetOutputPorts() {
        // execute
        Map<String, PortType> result = importer.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PORT_OUT_PROJECT));
        assertTrue(result.get(PORT_OUT_PROJECT) instanceof AvroPortType);
        assertSame(Project.SCHEMA$, ((AvroPortType) result.get(PORT_OUT_PROJECT)).getSchema());
    }
    
    @Test
    public void testRunWithoutStreamingFacade() {
        // given
        parameters.remove(IMPORT_FACADE_FACTORY_CLASS);
        
        // execute
        assertThrows(ServiceFacadeException.class, () -> importer.run(portBindings, conf, parameters));
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
