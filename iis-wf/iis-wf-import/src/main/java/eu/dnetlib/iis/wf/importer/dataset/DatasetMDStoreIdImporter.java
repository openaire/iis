package eu.dnetlib.iis.wf.importer.dataset;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_DATACITE_MDSTORE_IDS_CSV;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.Identifier;


/**
 * Process module writing dataset MDStore identifiers from parameters as {@link Identifier} avro records.
 * 
 * This step is required for further MDStore records retrieval parallelization. 
 * 
 * @author mhorst
 *
 */
public class DatasetMDStoreIdImporter implements Process {

    private static final String PORT_OUT_IDENTIFIER = "identifier";

    private final Logger log = Logger.getLogger(this.getClass());

    private static final Map<String, PortType> outputPorts = new HashMap<String, PortType>();

    {
        outputPorts.put(PORT_OUT_IDENTIFIER, new AvroPortType(Identifier.SCHEMA$));
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

        Preconditions.checkArgument(parameters.containsKey(IMPORT_DATACITE_MDSTORE_IDS_CSV),
                "unspecified MDStore identifiers, required parameter '%s' is missing!",
                IMPORT_DATACITE_MDSTORE_IDS_CSV);

        FileSystem fs = FileSystem.get(conf);
        FileSystemPath identifierOutput = new FileSystemPath(fs, portBindings.getOutput().get(PORT_OUT_IDENTIFIER));
        
        String mdStoresCSV = parameters.get(IMPORT_DATACITE_MDSTORE_IDS_CSV);
        if (StringUtils.isNotBlank(mdStoresCSV) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(mdStoresCSV)) {
            String[] mdStoreIds = StringUtils.split(mdStoresCSV, WorkflowRuntimeParameters.DEFAULT_CSV_DELIMITER);
            int counter = 0;
            for (String currentMdStoreId : mdStoreIds) {
                try (DataFileWriter<Identifier> mdStoreIdentifierWriter = DataStore.create(
                        identifierOutput, Identifier.SCHEMA$, DataStore.generateFileName(counter++))) {            
                    Identifier.Builder identifierBuilder = Identifier.newBuilder();
                    identifierBuilder.setId(currentMdStoreId);
                    mdStoreIdentifierWriter.append(identifierBuilder.build());
                }
            }
        } else {
            log.warn("got undefined mdstores list for datacite import, skipping!");
        }
    }

}
