package eu.dnetlib.iis.common.counter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

/**
 * Writer of {@link NamedCounters} object into a properties file.
 * 
 * @author madryk
 */
public class NamedCountersFileWriter {
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Writes {@link NamedCounters} as a properties file located under
     * provided filePath.
     * 
     * @throws IOException if writing to properties file resulted in an error
     */
    public void writeCounters(NamedCounters counters, String filePath) throws IOException {
        
        Properties counterProperties = buildPropertiesFromCounters(counters);
        
        File file = new File(filePath);
        try (OutputStream os = new FileOutputStream(file)) {
            
            counterProperties.store(os, null);
            
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private Properties buildPropertiesFromCounters(NamedCounters counters) {
        
        Properties properties = new Properties();
        
        for (String counterName : counters.counterNames()) {
            long count = counters.currentValue(counterName);
            properties.put(counterName, String.valueOf(count));
        }
        
        return properties;
    }
}
