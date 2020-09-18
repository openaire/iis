package eu.dnetlib.iis.common.java.jsonworkflownodes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import eu.dnetlib.iis.common.StaticResourceProvider;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.OrderedProperties;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.JsonUtils;
import eu.dnetlib.iis.common.java.porttype.AnyPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.report.test.ValueSpecMatcher;

/**
 * Avro datastores selective fields testing consumer. 
 * 
 * Requirements are provided as comma separated list of properties files holding field requirements for each individual input object. 
 * Properties keys are paths to the fields we want to test in given object, properties values are expected values (with special markup for null: $NULL$).
 * 
 * Number of input avro records should be equal to the number of provided properties files.
 *   
 * @author mhorst
 */
public class SelectiveTestingConsumer implements Process {

    private static final String EXPECTATION_FILE_PATHS = "expectation_file_paths";

    private static final String PORT_INPUT = "datastore";

    private final Map<String, PortType> inputPorts = new HashMap<String, PortType>();
    
    private final ValueSpecMatcher valueMatcher = new ValueSpecMatcher();
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public SelectiveTestingConsumer() {
        inputPorts.put(PORT_INPUT, new AnyPortType());
    }
    
    //------------------------ LOGIC ---------------------------------
    
    @Override
    public Map<String, PortType> getInputPorts() {
        return inputPorts;
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public void run(PortBindings portBindings, Configuration configuration, Map<String, String> parameters)
            throws Exception {

        Path inputRecordsPath = portBindings.getInput().get(PORT_INPUT);
        String expectationsPathsCSV = parameters.get(EXPECTATION_FILE_PATHS);
        Preconditions.checkArgument(StringUtils.isNotBlank(expectationsPathsCSV), 
                "no '%s' property value provided, field requirements were not specified!", EXPECTATION_FILE_PATHS);

        FileSystem fs = FileSystem.get(configuration);

        if (!fs.exists(inputRecordsPath)) {
            throw new RuntimeException(inputRecordsPath + " hdfs location does not exist!");
        }
        
        try (CloseableIterator<SpecificRecord> recordIterator = DataStore.getReader(new FileSystemPath(fs, inputRecordsPath))) {
            
            String[] recordExpectationsLocations = StringUtils.split(expectationsPathsCSV, ',');
            
            int recordCount = 0;
            
            while (recordIterator.hasNext()) {
                
                SpecificRecord record = recordIterator.next();
                recordCount++;

                if (recordCount > recordExpectationsLocations.length) {
                    throw new RuntimeException("got more records than expected: " + "unable to verify record no " + recordCount
                            + ", no field specification provided! Record contents: " + JsonUtils.toPrettyJSON(record.toString()));
                } else {
                    validateRecord(record, readExpectedValues(recordExpectationsLocations[recordCount - 1]));    
                }
            }
            
            if (recordCount < recordExpectationsLocations.length) {
                throw new RuntimeException(
                        "records count mismatch: " + "got: " + recordCount + " expected: " + recordExpectationsLocations.length);
            }
        }
    }
    
    //------------------------ PRIVATE ---------------------------------
    
    /**
     * Reads expected values from given location.
     */
    private Properties readExpectedValues(String location) throws IOException {
        Properties properties = new OrderedProperties();
        properties.load(StaticResourceProvider.getResourceInputStream(location.trim()));
        return properties;
    }
    
    /**
     * Validates record fields against specified expectations. RuntimeException is thrown when invalid.
     * 
     * @param record avro record to be validated
     * @param recordFieldExpectations set of field expectations defined as properties where key is field location and value is expected value
     */
    private void validateRecord(SpecificRecord record, Properties recordFieldExpectations) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        for (Entry<Object, Object> fieldExpectation : recordFieldExpectations.entrySet()) {
            
            String currentValue = PropertyUtils.getNestedProperty(record, (String)fieldExpectation.getKey()).toString();
            
            String expectedValue = fieldExpectation.getValue().toString();
            
            if (!valueMatcher.matches(currentValue, expectedValue)) {
                throw new RuntimeException(
                        "invalid field value for path: " + fieldExpectation.getKey() + ", expected: '" + fieldExpectation.getValue() + "', "
                                + "got: '" + currentValue + "' Full object content: " + JsonUtils.toPrettyJSON(record.toString()));
            }
        }
    }
    
}