package eu.dnetlib.iis.common.javamapreduce.hack;

import eu.dnetlib.iis.common.schemas.Identifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
@ExtendWith(MockitoExtension.class)
public class AvroSchemaGeneratorTest {

    @TempDir
    public File testFolder;
    
    @BeforeEach
    public void initEnv() {
        System.setProperty(OOZIE_ACTION_OUTPUT_FILENAME,
                testFolder.getAbsolutePath() + File.separatorChar + "test.properties");
    }
    
    // -------------------------------------- TESTS --------------------------------------
    
    @Test
    public void testMainNoArgs() {
        // execute
        assertThrows(RuntimeException.class, () -> AvroSchemaGenerator.main(new String[0]));
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
