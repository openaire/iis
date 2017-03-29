package eu.dnetlib.iis.common.report.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.common.java.jsonworkflownodes.PortSpecifications;
import eu.dnetlib.iis.common.java.jsonworkflownodes.PortSpecifications.SpecificationValues;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportEntry;

/**
 * Testing consumer useful for testing outputs of report generators.
 * 
 * Uses {@link ReportEntryMatcher} to compare lists of {@link ReportEntry} objects - the one generated and saved by the tested functionality
 * in an avro file and the one with expected values saved in a json file.
 * 
 *  
 * @author Łukasz Dumiszewski
*/

public class ReportTestingConsumer implements Process {
    
    private final PortSpecifications inputSpecs;
    
    private final ReportEntryMatcher matcher = new ReportEntryMatcher();
    
    /**
     * @param inputSpecifications specifications of input. Each element of
     * the array corresponds to a single specification. Single specification
     * conforms to the following template:
     * "{input port name, schema reference, path to JSON file in resources 
     * corresponding to the expected input data store}",
     * e.g. "{report, eu.dnetlib.iis.common.schemas.ReportEntry, 
     * eu/dnetlib/iis/core/examples/report.json}"
     */
    public ReportTestingConsumer(String[] inputSpecifications){
        inputSpecs = new PortSpecifications(inputSpecifications);
    }
    
    @Override
    public Map<String, PortType> getInputPorts() {
        return inputSpecs.getPortTypes();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return new HashMap<String, PortType>();
    }

    @Override
    public void run(PortBindings portBindings, Configuration configuration, Map<String, String> parameters) throws Exception {
        
        Map<String, Path> input = portBindings.getInput();
        
        FileSystem fs = FileSystem.get(configuration);
        
        for(Map.Entry<String, Path> e: input.entrySet()){
            
            SpecificationValues specs = inputSpecs.get(e.getKey());
            
            check(new FileSystemPath(fs, e.getValue()), specs);
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void check(FileSystemPath actualPath, SpecificationValues specs) throws IOException{
    
        List<ReportEntry> expectedEntrySpecs = JsonUtils.convertToList(specs.getJsonFilePath(), ReportEntry.SCHEMA$, ReportEntry.class);
        
        List<ReportEntry> actualEntries = DataStore.read(actualPath, ReportEntry.SCHEMA$);
        
        matcher.checkMatch(actualEntries, expectedEntrySpecs);
    }
}
