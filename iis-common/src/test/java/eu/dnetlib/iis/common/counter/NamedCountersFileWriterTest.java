package eu.dnetlib.iis.common.counter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class NamedCountersFileWriterTest {

    private NamedCountersFileWriter countersFileWriter = new NamedCountersFileWriter();
    
    public File tempFolder;

    private String counterName1 = "COUNTER_1";
    
    private String counterName2 = "COUNTER_2";

    @BeforeEach
    public void beforeEach() throws IOException {
        tempFolder = Files.createTempDirectory(this.getClass().getSimpleName()).toFile();
    }

    //------------------------ TESTS --------------------------
    
    @Test
    public void writeCounters() throws IOException {
        
        // given
        
        NamedCounters namedCounters = new NamedCounters(new String[] { counterName1, counterName2 });
        namedCounters.increment(counterName1, 4L);
        namedCounters.increment(counterName2, 2L);
        
        // execute
        
        countersFileWriter.writeCounters(namedCounters, tempFolder.getPath() + "/counters.properties");
        
        // assert
        
        Properties actualProperties = loadProperties(new File(tempFolder, "counters.properties"));
        
        Properties expectedProperties = new Properties();
        expectedProperties.put(counterName1, "4");
        expectedProperties.put(counterName2, "2");
        
        assertEquals(expectedProperties, actualProperties);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Properties loadProperties(File propertiesFile) throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        
        try (Reader reader = new FileReader(propertiesFile)) {
            properties.load(reader);
        }
        
        return properties;
    }
    
}
