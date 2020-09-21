package eu.dnetlib.iis.common.java.jsonworkflownodes;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
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
    
    /**
     * 
     * @param inputSpecifications specifications of input. Each element
     * of array corresponds to single specification. Single specification
     * must be in the following template:<br/>
     * <code>{PORT_NAME, PATH_TO_JSON}</code><br/>
     * where:<br/>
     * <code>PORT_NAME</code> - name of the port (user of this class must define also input port with the same name)<br/>
     * <code>PATH_TO_JSON</code> - path to json file in resources. Process will check if input json under
     *      port with name <code>PORT_NAME</code> is equal to this json file.<br/>
     *   
     */
    public TestingJsonConsumer(String[] inputSpecifications) {
        StringPortSpecificationExtractor specificationExtractor = new StringPortSpecificationExtractor(new String[]{"[\\w\\.\\/_]+"});
        
        for (String inputSpecification : inputSpecifications) {
            PortSpecification portSpec = specificationExtractor.getSpecification(inputSpecification);
            String jsonFilePath = portSpec.getProperties()[0];
            
            inputPortsSpecification.put(portSpec.getName(), new JsonPortSpecification(portSpec.getName(), jsonFilePath));
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
        try (InputStreamReader reader = ClassPathResourceProvider.getResourceReader(specs.getJsonFilePath())) {
            expectedJsonElement = parser.parse(reader);
        }
        
        JsonElement actualJsonElement = null;
        try (InputStreamReader reader2 = new InputStreamReader(actualPath.getInputStream())) {
            actualJsonElement = parser.parse(reader2);
        }
        
        
        assertEquals(expectedJsonElement, actualJsonElement);
    }

}
