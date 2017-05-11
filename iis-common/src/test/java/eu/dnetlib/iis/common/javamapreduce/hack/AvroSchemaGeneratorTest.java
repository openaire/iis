package eu.dnetlib.iis.common.javamapreduce.hack;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.common.schemas.Identifier;

/**
 * @author mhorst
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class AvroSchemaGeneratorTest {
    
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();
    
    @Before
    public void initEnv() {
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME, 
                testFolder.getRoot().getAbsolutePath() + File.separatorChar + "test.properties");
    }
    
    // -------------------------------------- TESTS --------------------------------------
    
    @Test(expected=RuntimeException.class)
    public void testMainNoArgs() throws Exception {
        // execute
        AvroSchemaGenerator.main(new String[0]);
    }
    
    @Test
    public void testMain() throws Exception {
        // given
        String className = Identifier.class.getCanonicalName();
        
        // execute
        AvroSchemaGenerator.main(new String[] {className});
        
        // assert
        Properties properties = getStoredProperties();
        assertNotNull(properties);
        assertEquals(1, properties.size());
        assertTrue(properties.containsKey(className));
        assertEquals(Identifier.SCHEMA$.toString(), properties.getProperty(className));
    }

    // -------------------------------------- PRIVATE --------------------------------------
    
    private Properties getStoredProperties() throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME)));
        return properties;
    }
}
