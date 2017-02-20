package eu.dnetlib.iis.wf.importer;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.Identifier;


/**
 * Process module writing identifiers provided in input parameter as {@link Identifier} avro records.
 * 
 * This step is required for further records retrieval parallelization. 
 * 
 * @author mhorst
 *
 */
public abstract class AbstractIdentifierDatastoreBuilder implements Process {

    private static final String PORT_OUT_IDENTIFIER = "identifier";

    private final Logger log = Logger.getLogger(this.getClass());

    private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();

    {
        outputPorts.put(PORT_OUT_IDENTIFIER, new AvroPortType(Identifier.SCHEMA$));
    }

    /**
     * Parameter name holding CSV of identifiers to be written.
     */
    private final String identifiersParamName;
    
    /**
     * Parameter name holding CSV of identifiers to be excluded.
     */
    private final String blacklistedIdentifiersParamName;
    
    
    
    // ------------------------ CONSTRUCTOR --------------------------
    
    /**
     * @param identifiersParamName parameter name holding CSV of identifiers to be written
     * @param blacklistedIdentifiersParamName parameter name holding CSV of identifiers to be excluded, functionality disabled when set to null
     */
    public AbstractIdentifierDatastoreBuilder(String identifiersParamName, String blacklistedIdentifiersParamName) {
        this.identifiersParamName = identifiersParamName;
        this.blacklistedIdentifiersParamName = blacklistedIdentifiersParamName;
    }
    
    // ------------------------ LOGIC --------------------------

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return outputPorts;
    }

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {

        Preconditions.checkArgument(parameters.containsKey(identifiersParamName),
                "unspecified identifiers, required parameter '%s' is missing!", identifiersParamName);

        Set<String> blacklistedIdentifiers = Collections.emptySet();
        if (StringUtils.isNotBlank(blacklistedIdentifiersParamName)) {
            String blacklistedIdentifiersCSV = parameters.get(blacklistedIdentifiersParamName);
            if (StringUtils.isNotBlank(blacklistedIdentifiersCSV)) {
                blacklistedIdentifiers = Sets.newHashSet(StringUtils.split(blacklistedIdentifiersCSV, DEFAULT_CSV_DELIMITER));
            }     
        }

        FileSystem fs = FileSystem.get(conf);
        FileSystemPath identifierOutput = new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_IDENTIFIER));
        
        int counter = 0;
        
        String identifiersCSV = parameters.get(identifiersParamName);
        if (StringUtils.isNotBlank(identifiersCSV) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(identifiersCSV)) {
            String[] identifiers = StringUtils.split(identifiersCSV, WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER);
            
            for (String currentId : identifiers) {
                if (!blacklistedIdentifiers.contains(currentId)) {
                    try (DataFileWriter<Identifier> mdStoreIdentifierWriter = DataStore.create(
                            identifierOutput, Identifier.SCHEMA$, DataStore.generateFileName(counter++))) {            
                        Identifier.Builder identifierBuilder = Identifier.newBuilder();
                        identifierBuilder.setId(currentId);
                        mdStoreIdentifierWriter.append(identifierBuilder.build());
                    }    
                } else {
                    log.info("skipping blacklisted id: " + currentId);
                }
            }
        }
        
        if (counter==0) {
//          writing empty datastore required for further processing
            DataStore.create(identifierOutput, Identifier.SCHEMA$);
        }
    }

}
