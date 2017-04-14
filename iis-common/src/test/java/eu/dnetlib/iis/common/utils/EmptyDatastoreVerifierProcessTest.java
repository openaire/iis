package eu.dnetlib.iis.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class EmptyDatastoreVerifierProcessTest {

    
    private Configuration conf = null;
    
    private Map<String, String> parameters = null;
    
    @Mock
    private CloseableIterator<?> iterator;
    
    private EmptyDatastoreVerifierProcess process = new EmptyDatastoreVerifierProcess() {
        
        @Override
        protected CloseableIterator<?> getIterator(Configuration conf, Path path) throws IOException {
            return iterator;
        }
        
    };
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Before
    public void init() {
        parameters = new HashMap<>();
        
        System.setProperty(EmptyDatastoreVerifierProcess.OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getRoot().getAbsolutePath() + File.separatorChar + "test.properties");
    }

    // --------------------------------- TESTS -------------------------------------
    
    @Test
    public void testGetInputPorts() {
        // execute & assert
        assertNotNull(process.getInputPorts());
        assertEquals(1, process.getInputPorts().size());
        assertNotNull(process.getInputPorts().get(EmptyDatastoreVerifierProcess.INPUT_PORT_NAME));
        assertEquals(AnyPortType.class, process.getInputPorts().get(EmptyDatastoreVerifierProcess.INPUT_PORT_NAME).getClass());
    }
    
    @Test
    public void testGetOutputPorts() {
        // execute & assert
        assertNotNull(process.getOutputPorts());
        assertEquals(0, process.getOutputPorts().size());
    }
    
    @Test(expected=InvalidParameterException.class)
    public void testVerifyEmptyDatastoreWithoutInput() throws Exception {
        // execute
        process.run(new PortBindings(Collections.emptyMap(), Collections.emptyMap()), conf, parameters);
    }
    
    @Test
    public void testVerifyEmptyDatastore() throws Exception {
        // given
        Map<String, Path> input = new HashMap<>();
        input.put(EmptyDatastoreVerifierProcess.INPUT_PORT_NAME, new Path("/irrelevant/location/as/it/will/be/mocked"));
        PortBindings portBindings = new PortBindings(input, Collections.emptyMap());
        doReturn(false).when(iterator).hasNext();
        
        // execute
        process.run(portBindings, conf, parameters);
        
        // assert
        assertTrue(verifyAndGetResult());
    }
    
    @Test
    public void testVerifyNonEmptyDatastore() throws Exception {
        // given
        Map<String, Path> input = new HashMap<>();
        input.put(EmptyDatastoreVerifierProcess.INPUT_PORT_NAME, new Path("/irrelevant/location/as/it/will/be/mocked"));
        PortBindings portBindings = new PortBindings(input, Collections.emptyMap());
        doReturn(true).when(iterator).hasNext();
        
        // execute
        process.run(portBindings, conf, parameters);
        
        // assert
        assertFalse(verifyAndGetResult());
    }
    
    // --------------------------------- PRIVATE -------------------------------------
    
    private boolean verifyAndGetResult() throws FileNotFoundException, IOException {
        Properties properties = getStoredProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        return Boolean.parseBoolean((String) properties.get(EmptyDatastoreVerifierProcess.OUTPUT_PROPERTY_IS_EMPTY));
    }
    
    private Properties getStoredProperties() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(System.getProperty(EmptyDatastoreVerifierProcess.OOZIE_ACTION_OUTPUT_FILENAME)));
        return properties;
    }
    
}
