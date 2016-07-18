package eu.dnetlib.iis.common.java.jsonworkflownodes;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.ProcessWrapper;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.jsonworkflownodes.StringPortSpecificationExtractor.PortSpecification;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * Workflow java node that checks if input path contains json file
 * that is equal to json provided as parameter.
 * 
 * @author madryk
 */
public class TestingJsonConsumer implements Process {

    private final Map<String, JsonPortSpecification> inputPortsSpecification = Maps.newHashMap();
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public TestingJsonConsumer(String[] inputSpecifications) {
        StringPortSpecificationExtractor specificationExtractor = new StringPortSpecificationExtractor(new String[]{"[\\w\\.\\/_]+"});
        
        for (String inputSpecification : inputSpecifications) {
            PortSpecification portSpec = specificationExtractor.getSpecification(inputSpecification);
            String jsonFilePath = portSpec.properties[0];
            
            inputPortsSpecification.put(portSpec.name, new JsonPortSpecification(portSpec.name, jsonFilePath));
        }
    }
    
    
    //------------------------ LOGIC --------------------------
    
    @Override
    public Map<String, PortType> getInputPorts() {
        Map<String, PortType> ports = new HashMap<String, PortType>();
        for(Map.Entry<String, JsonPortSpecification> e: inputPortsSpecification.entrySet()){
            ports.put(e.getKey(), new AnyPortType());
        }
        return ports;
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return new HashMap<String, PortType>();
    }
    
    
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        
        Map<String, Path> input = portBindings.getInput();
        FileSystem fs = FileSystem.get(conf);
        for(Map.Entry<String, Path> e: input.entrySet()) {
            
            JsonPortSpecification specs = inputPortsSpecification.get(e.getKey());
            check(new FileSystemPath(fs, e.getValue()), specs);
            
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void check(FileSystemPath actualPath, JsonPortSpecification specs) throws IOException {
        JsonParser parser = new JsonParser();
        
        JsonElement expectedJsonElement = null;
        try (InputStreamReader reader = new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(specs.getJsonFilePath()))) {
            expectedJsonElement = parser.parse(reader);
        }
        
        JsonElement actualJsonElement = null;
        try (InputStreamReader reader2 = new InputStreamReader(actualPath.getInputStream())) {
            actualJsonElement = parser.parse(reader2);
        }
        
        
        assertEquals(expectedJsonElement, actualJsonElement);
    }

}
