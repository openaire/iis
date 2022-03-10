package eu.dnetlib.iis.wf.referenceextraction.researchinitiative;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.ResearchInitiativeMetadata;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process building research initiatives database reading {@link DataSetReference} input avro records.
 *
 * @author mhorst
 */
public class ResearchInitiativeDBBuilder extends AbstractDBBuilder<ResearchInitiativeMetadata> {

    private final static String PARAM_SCRIPT_LOCATION = "scriptLocation";

    // -------------------------- CONSTRUCTORS ------------------------------

    public ResearchInitiativeDBBuilder() {
        super(ResearchInitiativeMetadata.SCHEMA$, "initiatives", "initiatives_db");
    }

    // -------------------------- LOGIC -------------------------------------

    @Override
    public ProcessExecutionContext initializeProcess(Map<String, String> parameters) throws IOException {
        String scriptLocation = parameters.get(PARAM_SCRIPT_LOCATION);
        Preconditions.checkArgument(StringUtils.isNotBlank(scriptLocation),
                "sql script location not provided, '%s' parameter is missing!", PARAM_SCRIPT_LOCATION);

        String targetDbLocation = System.getProperty("java.io.tmpdir") + File.separatorChar + "initiatives.db";
        File targetDbFile = new File(targetDbLocation);
        targetDbFile.setWritable(true);

        return new ProcessExecutionContext(
                Runtime.getRuntime().exec("python3 scripts/madis/mexec.py -w " + targetDbLocation + " -f " + scriptLocation),
                targetDbFile);
    }

}
