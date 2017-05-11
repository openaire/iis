package eu.dnetlib.iis.wf.referenceextraction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonStreamReader;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.importer.schemas.Project;

/**
 * {@link AbstractDBBuilder} test class.
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DBBuilderTest {
    
    private static final Schema SCHEMA_PROJECT = Project.SCHEMA$;
    
    private static final String PORT_NAME_INPUT = "input";
    
    private static final String PORT_NAME_OUTPUT = "output";

    private final PortBindings portBindings = new PortBindings(new HashMap<>(), new HashMap<>());
    
    private final Configuration conf = null;
    
    private final Map<String, String> parameters = null;
    
    private File outputFile; 
    
    private Project firstProject = buildProject("id1", "text1");
    
    private Project secondProject = buildProject("id2", "text2");
    
    private List<Project> projects;

    private AbstractDBBuilder<Project> dbBuilder;
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Mock
    private FileSystemFacade fileSystemFacade;
    
    @Mock
    private Process process;
    
    @Before
    public void initializeBuilder() {
        
        projects = Arrays.asList(new Project[] {firstProject, secondProject});
        
        outputFile = new File(testFolder.getRoot(),"tmpfile");
        
        dbBuilder = new AbstractDBBuilder<Project>(
                (conf) -> {
                    return fileSystemFacade;
                },
                SCHEMA_PROJECT, PORT_NAME_INPUT, PORT_NAME_OUTPUT) {

            @Override
            protected ProcessExecutionContext initializeProcess(Map<String, String> parameters) throws IOException {
                return new ProcessExecutionContext(process, outputFile);
            }
            
            @Override
            protected CloseableIterator<Project> getInputRecordsIterator(FileSystemPath fileSystemPath) throws IOException {
                return new CloseableIterator<Project>() {

                    final Iterator<Project> it = projects.iterator();
                    
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Project next() {
                        return it.next();
                    }

                    @Override
                    public void close() throws IOException {
                        // does nothing
                    }
                };
            }
            
        };
    }
    
    @Test
    public void testNonZeroExitValue() throws Exception {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String errorMessage = "custom error message related to non zero exit value";
        
        doReturn(outputStream).when(fileSystemFacade).create(any());
        doReturn(null).when(fileSystemFacade).getFileSystem();
        doReturn(new FileOutputStream(outputFile)).when(process).getOutputStream();
        doReturn(1).when(process).exitValue();
        doReturn(new ByteArrayInputStream(errorMessage.getBytes("utf8"))).when(process).getErrorStream();
        
        try {
            // execute
            dbBuilder.run(portBindings, conf, parameters);
            fail("exception was expected!");
        } catch (RuntimeException e) {
            // assert
            assertTrue(e.getMessage().contains(errorMessage));
            assertEquals(0, outputStream.size());
        }
    }
    
    @Test
    public void testException() throws Exception {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String errorMessage = "custom error message after exception";
        Throwable exception = new RuntimeException(errorMessage);
        
        doReturn(outputStream).when(fileSystemFacade).create(any());
        doReturn(null).when(fileSystemFacade).getFileSystem();
        doReturn(new FileOutputStream(outputFile)).when(process).getOutputStream();
        doReturn(0).when(process).exitValue();
        doThrow(exception).when(process).waitFor();
        doReturn(new ByteArrayInputStream(errorMessage.getBytes("utf8"))).when(process).getErrorStream();
        
        try {
            // execute
            dbBuilder.run(portBindings, conf, parameters);
            fail("exception was expected!");
        } catch (IOException e) {
            // assert
            assertTrue(e.getMessage().contains(errorMessage));
            assertTrue(exception == e.getCause());
            assertEquals(0, outputStream.size());
        }
    }
    
    @Test
    public void testEmptyInput() throws Exception {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        projects = Collections.emptyList();
        
        doReturn(outputStream).when(fileSystemFacade).create(any());
        doReturn(null).when(fileSystemFacade).getFileSystem();
        doReturn(new FileOutputStream(outputFile)).when(process).getOutputStream();
        doReturn(0).when(process).exitValue();
        
        dbBuilder.run(portBindings, conf, parameters);
        assertEquals(0, outputStream.size());
    }
    
    @Test
    public void test() throws Exception {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        
        doReturn(outputStream).when(fileSystemFacade).create(any());
        doReturn(null).when(fileSystemFacade).getFileSystem();
        doReturn(0).when(process).exitValue();
        doReturn(new FileOutputStream(outputFile)).when(process).getOutputStream();
        
        // execute
        dbBuilder.run(portBindings, conf, parameters);
        
        // assert
        try (JsonStreamReader<Project> reader = new JsonStreamReader<Project>(
                Project.SCHEMA$, new ByteArrayInputStream(outputStream.toByteArray()), Project.class)) {
            assertTrue(reader.hasNext());
            Project receivedProject = reader.next();
            assertEquals(firstProject, receivedProject);
            assertTrue(reader.hasNext());
            receivedProject = reader.next();
            assertEquals(secondProject, receivedProject);
            assertFalse(reader.hasNext());
        }
    }

    @Test
    public void testGetInputPorts() throws Exception {
        // execute
        Map<String, PortType> result = dbBuilder.getInputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PORT_NAME_INPUT));
        assertTrue(result.get(PORT_NAME_INPUT) instanceof AvroPortType);
        assertTrue(SCHEMA_PROJECT == ((AvroPortType)result.get(PORT_NAME_INPUT)).getSchema());
    }
    
    @Test
    public void testGetOutputPorts() throws Exception {
        // execute
        Map<String, PortType> result = dbBuilder.getOutputPorts();
        
        // assert
        assertNotNull(result);
        assertNotNull(result.get(PORT_NAME_OUTPUT));
        assertTrue(result.get(PORT_NAME_OUTPUT) instanceof AnyPortType);
    }
    
    // --------------------------------- PRIVATE -----------------------------------------
    
    private Project buildProject(String id, String jsonExtraInfo) {
        return Project.newBuilder().setId(id).setJsonextrainfo(jsonExtraInfo).build();
    }
    
}
